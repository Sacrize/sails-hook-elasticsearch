# Sails Hook Elasticsearch
Integration with elasticsearch. Flexible data indexing and searching with query building.

## Getting Started
Install it via npm:
```bash
npm install sails-hook-elasticsearch --save
```
## Configuration
Configure `config/elasticsearch.js`:
```javascript
module.exports.elasticsearch = {
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
};
```
Elasticsearch client configuration [here](https://www.elastic.co/guide/en/elasticsearch/client/javascript-api/current/client-configuration.html#client-configuration).

## Indexes
In the location `/api/indexes` you need to create an index configuration file.
```javascript
// Example: Invoice.js

module.exports = {

  /**
   * Index configuration.
   */
  config: {
    index: 'invoice',
    body: {
      mappings: {
        properties: {
          currency: {
            type: 'keyword',
          },
        }
      },
      settings: {

      },
    }
  },

  /**
   * Collect objects for indexing.
   * @param index
   */
  populate: function (index) {
    index({
      currency: 'test',
      id: 1,
    });
    index({
      currency: 'test2',
      id: 2,
    });
  },

  /**
   * Convert the object to an index write form.
   * @param {object} data
   * @returns {object}
   */
  serialize: function (data) {
    return {
      id: data.id,
      currency: data.currency,
    };
  },

  /**
   * Building search queries.
   */
  query: {
    filter: {
      currency: function (value) {
        let term = _.isArray(value) ? 'terms': 'term';
        return {
          [term]: {
            'currency': value,
          },
        };
      }
    },
    should: {

    },
    must: {

    },
  }
};
```
 
## Available methods
- index(index, data, force)
    - index: name of index
    - data: data to index
    - force: push to queue = false or force index = true
- collect({ index, filter, order, page, limit, raw, })
    - index: name of index
    - filter: object with filters
    - order: array with elastic search sorting rules
    - page: number of page
    - limit: number per page
    - raw: override parameters to the original search method

## License

[MIT](./LICENSE)
