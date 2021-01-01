const { Client } = require('@elastic/elasticsearch');
const path = require('path');
const fs = require('fs');
const _ = require('lodash');

module.exports = function (sails) {

  let config;
  let client;
  let indices = [];

  sails.on('ready', function () {
    _checkDependencies();
  });

  return {
    defaults: {
      __configKey__: {
        indicesPath: 'indices',
        queue: {
          enable: true,
          name: 'elasticsearch',
          executor: {
            batch: 10,
            cronJob: {
              name: 'elasticsearchQueueExecutor',
              schedule: '*/5 * * * * *'
            },
          },
        },
        client: {
          node: 'http://127.0.0.1:9200',
          maxRetries: 10,
          requestTimeout: 60000,
        },
        schema: {
          /**
           * Index configuration.
           */
          config: {
            index: '',
            body: {
              mappings: {
                properties: {},
              },
              settings: {},
            },
          },
          /**
           * Collect objects for indexing.
           * @param index
           */
          populate: function (index) {},
          /**
           * Convert the object to an index write form.
           * @param {object} data
           * @returns {object}
           */
          serialize: function (data) {},
          /**
           * Building search queries.
           */
          query: {
            filter: {},
            should: {},
            must: {},
          }
        }
      },
    },
    configure: function () {
      config = sails.config[this.configKey];
      if (config.queue.enable) {
        sails.config.queues = sails.config.queues || {};
        sails.config.queues[config.queue.name] = {};
        sails.config.cron = sails.config.cron || {};
        sails.config.cron[config.queue.executor.cronJob.name] = {
          schedule: config.queue.executor.cronJob.schedule,
          onTick: function () {
            _executeFromQueue();
          }
        }
      }
    },
    initialize: function (cb) {
      if (!config.client.node) {
        return cb();
      }
      sails.log.info('Initializing hook (`sails-hook-elasticsearch`)');
      client = new Client(config.client);
      _initIndices()
        .then(() => cb())
        .catch(e => {
          sails.log.debug(JSON.stringify(e));
          sails.log.error(e);
        });
    },

    index: function (index, data, force) {
      if (typeof force === 'undefined') { force = false; }
      return _getModelByIndex(index).then((model) => {
        let serializedData = { ...data, };
        if (model.serialize) {
          serializedData = model.serialize(serializedData);
        }
        return _indexData(model, serializedData, force);
      });
    },
    collect: _collect,
    count: _count,
    reCreateIndices: _reCreateIndices,
    removeFromIndex: _removeFromIndex,
    existsInIndex: _existsInIndex,
    populateIndices: _populateIndices,
  }

  /**
   *
   * @param {string} index
   * @param {object} [filter]
   * @param {object} [order]
   * @param {number} [page]
   * @param {number} [limit]
   * @param {string[]} [select]
   * @param {object} [raw]
   * @returns {Promise<{hits: *|number, total: *|[]}>}
   * @private
   */
  function _collect({ index, filter, order, page, limit, select, raw, }) {
    raw = raw || {};
    filter = filter || {};
    order = order || {};
    limit = limit || 9999;
    return _getModelByIndex(index)
      .then((model) => {
        let searchQuery = {
          index,
          body: _buildQuery(model, filter, order),
          track_scores: true,
          size: limit && limit > 0 ? limit : 9999,
          ...raw,
        }
        if (_.isArray(select) && select.length) {
          searchQuery._source = select;
        }
        if (page && searchQuery.size) {
          searchQuery.from = (page - 1) * searchQuery.size;
        }
        return client.search(searchQuery)
          .then(({ body }) => body.hits)
          .catch((error) => {
            sails.log.debug(JSON.stringify(searchQuery));
            sails.log.error(error);
          })
          .then((result) => {
            return {
              hits: result ? result.hits : 0,
              total: result ? result.total.value : [],
            }
          });
      })
  }

  /**
   *
   * @param {string} index
   * @param {object} [filter]
   * @param {object} [order]
   * @param {number} [page]
   * @param {number} [limit]
   * @param {string[]} [select]
   * @param {object} [raw]
   * @returns {Promise<{hits: *|number, total: *|[]}>}
   * @private
   */
  function _count({ index, filter }) {
    filter = filter || {};
    return _getModelByIndex(index)
      .then((model) => {
        let searchQuery = {
          index,
          body: _buildQuery(model, filter),
        }
        return client.count(searchQuery)
          .then(({ body }) => body.count)
          .catch((error) => {
            sails.log.debug(JSON.stringify(searchQuery));
            sails.log.error(error);
          })
          .then((result) => {
            return {
              total: result,
            }
          });
      })
  }

  /**
   * Builds a query based on filters.
   * @param {object} model
   * @param {object} where
   * @param {object} [order]
   * @returns {object} elasticsearch query
   * @private
   */
  function _buildQuery(model, where, order) {
    let q = {};
    let sort = [];
    let must = [];
    let filter = [];
    let should = [];

    // not changing _score
    let filterFields = Object.keys(model.query.filter || {});
    for (let filterField of filterFields) {
      if (_.has(where, filterField) && _.has(model.query.filter, filterField)) {
        filter.push(model.query.filter[filterField](where[filterField]));
      }
    }
    // changing _score
    let shouldFields = Object.keys(model.query.should || {});
    for (let shouldField of shouldFields) {
      if (_.has(where, shouldField) && _.has(model.query.should, shouldField)) {
        should.push(model.query.should[shouldField](where[shouldField]));
      }
    }
    let mustFields = Object.keys(model.query.must || {});
    for (let mustField of mustFields) {
      if (_.has(where, mustField) && _.has(model.query.must, mustField)) {
        must.push(model.query.must[mustField](where[mustField]));
      }
    }

    filter = filter.filter(m => !!m);
    should = should.filter(m => !!m);
    must   =   must.filter(m => !!m);

    if (model.query.compute) {
      model.query.compute({ where, filter, should, must, sort, })
    }

    if (_.isArray(order)) {
      sort = sort.concat(order);
    }
    if (sort.length) {
      q['sort'] = sort;
    }
    if (must.length) {
      q['query'] = q['query'] || {};
      q['query']['bool'] = q['query']['bool'] || {};
      q['query']['bool']['must'] = must;
    }
    if (filter.length) {
      q['query'] = q['query'] || {};
      q['query']['bool'] = q['query']['bool'] || {};
      q['query']['bool']['filter'] = filter;
    }
    if (should.length) {
      q['query'] = q['query'] || {};
      q['query']['bool'] = q['query']['bool'] || {};
      q['query']['bool']['should'] = should;
    }
    if (!must.length && !filter.length && !should.length) {
      q['query'] = q['query'] || {};
      q['query']['match_all'] = {};
    }
    return q;
  }

  /**
   * Returns the internal index object.
   * @param {string} index
   * @returns {Promise<object>}
   * @private
   */
  function _getModelByIndex(index) {
    return new Promise((resolve, reject) => {
      let model = indices.find(i => i.config.index === index);
      if (!model) {
        reject(new Error('Not found model for index `'+index+'`'));
      } else {
        resolve(model);
      }
    });
  }

  /**
   * Populates indexes with data.
   * @param {boolean} [force]
   * @private
   */
  function _populateIndices(force) {
    for (let model of indices) {
      if (!model.populate) {
        sails.log.error(new Error(`Not found populate() method for index '${model.config.index}'`))
        continue;
      }
      model.populate(function (data) {
        let serializedData = { ...data, };
        if (model.serialize) {
          serializedData = model.serialize(serializedData);
        }
        _indexData(model, serializedData, force)
          .catch((error) => {
            sails.log.debug(JSON.stringify(error));
            sails.log.error(error);
          });
      })
    }
  }

  /**
   * Adds an object to the indexing queue or forces indexing immediately.
   * @param {object} model
   * @param {object} data
   * @param {boolean} [force]
   * @returns {Promise<*>|*}
   * @private
   */
  function _indexData(model, data, force) {
    if (!force && sails.hooks.queues.isReady(config.queue.name)) {
      return sails.hooks.queues.push(config.queue.name, { index: model.config.index, body: data, });
    } else {
      return _processIndexData(model.config.index, data);
    }
  }

  /**
   * Pulls objects out of the queue and indexes them.
   * @private
   */
  function _executeFromQueue() {
    if (!sails.hooks.queues.isReady(config.queue.name)) {
      return;
    }
    for (let i = 0; i < config.queue.executor.batch; i++) {
      sails.hooks.queues.pop(config.queue.name)
        .then((res) => {
          if (res && res.message) {
            _processIndexData(res.message.index, res.message.body)
              .catch(e => {
                sails.log.debug(JSON.stringify(e));
                sails.log.error(e);
              });
          }
        });
    }
  }

  /**
   * Indexes the data.
   * @param {string} index
   * @param {object} body
   * @returns {Promise<*>}
   * @private
   */
  function _processIndexData(index, body) {
    return client.index({ index, id: body.id, body, }).then(({ body }) => body.result);
  }

  /**
   * Checks if an object exists in the index.
   * @param {string} index
   * @param {string} id
   * @returns {Promise<*>}
   * @private
   */
  function _existsInIndex(index, id) {
    return client.exists({ id, index, }).then(({ body }) => body);
  }

  /**
   * Removes an object from the index.
   * @param {string} index
   * @param {string} id
   * @returns Promise
   * @private
   */
  function _removeFromIndex(index, id) {
    return client.delete({ id, index, refresh: 'true', });
  }

  /**
   * Refreshing the index.
   * @param index
   * @returns {Promise<ApiResponse<Record<string, any>, Context>>}
   * @private
   */
  function _refreshIndex(index) {
    return client.indices.refresh({ index, });
  }

  /**
   * Refreshing the indexes.
   * @returns {Promise<ApiResponse<Record<string, any>, Context>>}
   * @private
   */
  function _refreshIndices() {
    return client.indices.refresh();
  }

  /**
   * Reads index configurations and creates or updates them on startup.
   * @returns {Promise<void>}
   * @private
   */
  function _initIndices() {
    return new Promise((resolve, reject) => {
      indices = [];
      let normalizedPath = path.join(sails.config.appPath + '/api/', config.indicesPath);
      if (!fs.existsSync(normalizedPath)) {
        return reject(new Error('Not found `'+config.indicesPath+'` catalog'));
      }
      let promises = [];
      fs.readdirSync(normalizedPath).forEach(fileName => {
        if (!fileName.includes('.js')) {
          return;
        }
        let model = require(normalizedPath + '/' + fileName) || {};
        model = _.merge({}, config.schema, model);
        let name = model.config.index || fileName.replace(/\.js/, '').toLowerCase();
        sails.log.info('Initializing elasticsearch index `'+name+'`');
        model.config.index = name;
        indices.push(model);
        promises.push(_initIndex(model.config));
      });
      Promise.all(promises).then(() => resolve()).catch((e) => reject(e));
    })
  }

  /**
   * Creates or updates an existing index.
   * @param {object} config
   * @returns {Promise<ApiResponse<Record<string, *>, Context>>}
   * @private
   */
  function _initIndex(config) {
    return _indexExists(config.index)
      .then(exists => {
        if (exists) {
          return _updateIndex(config)
              .catch((e) => {
                sails.log.debug('The schema could not be updated. Rebuilding...');
                return _reCreateIndices();
              });
        } else {
          return _createIndex(config);
        }
      });
  }

  /**
   * Updates an existing index.
   * @param {object} config
   * @returns {Promise}
   * @private
   */
  function _updateIndex(config) {
    return Promise.resolve()
        .then(() => {
          return _updateIndexMappings(config.index, config.body.mappings)
        })
        .then(() => {
          return _updateIndexSettings(config.index, config.body.settings)
        })
  }

  /**
   * Updates mappings of existing index.
   * @param {string} index
   * @param {object} mappings
   * @returns {Promise}
   * @private
   */
  function _updateIndexMappings(index, mappings) {
    if (_.isEmpty(mappings)) { return Promise.resolve(); }
    return client.indices.putMapping({ index, body: mappings, });
  }

  /**
   * Updates settings of existing index.
   * @param {string} index
   * @param {object} settings
   * @returns {Promise}
   * @private
   */
  function _updateIndexSettings(index, settings) {
    if (_.isEmpty(settings)) { return Promise.resolve(); }
    return client.indices.close({ index, })
      .then(() => {
        return client.indices.putSettings({ index, body: settings, });
      })
      .then(() => {
        return client.indices.open({ index, });
      });
  }

  /**
   * Creates an index.
   * @param {object} params
   * @returns {TransportRequestPromise<ApiResponse<Record<string, any>, Context>>}
   * @private
   */
  function _createIndex(params) {
    return client.indices.create(params);
  }

  /**
   * Removes all indexes and reinitializes them.
   * @returns {Promise<void>}
   * @private
   */
  function _reCreateIndices() {
    return client.indices.delete({ index: indices.map(i => i.config.index).join(','), })
      .then(() => _initIndices())
  }

  /**
   * Checks if the index was created.
   * @param {string} index
   * @returns {Promise<ApiResponse<boolean, Context>>}
   * @private
   */
  function _indexExists(index) {
    return client.indices.exists({ index, }).then(({ body }) => !!body);
  }

  /**
   * Required hooks to be installed in the project.
   * @private
   */
  function _checkDependencies() {
    let modules = [];
    if (config.queue.enable) {
      if (!sails.hooks.cron) {
        modules.push('sails-hook-cron');
      }
      if (!sails.hooks.queues) {
        modules.push('sails-hook-custom-queues');
      }
    }
    if (modules.length) {
      throw new Error('To use hook `sails-hook-elasticsearch`, you need to install the following modules: ' + modules.join(', '));
    }
  }

}
