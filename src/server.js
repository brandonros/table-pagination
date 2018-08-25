var Promise = require('bluebird');
var sqlite = require('sqlite');
var squel = require('squel');
var express = require('express');
var camelcaseKeys = require('camelcase-keys');
var snake = require('to-snake-case');
var camelCase = require('camelcase');

function buildDataQuery(req, tableName) {
  var limit = req.query.limit || 100;
  var fields = req.query.fields || [{name: '*'}];

  var sql = squel.select()
    .from(tableName);

  fields.forEach(function(field) {
    sql.field(snake(field.name));
  });

  if (req.query.filters) {
    req.query.filters.forEach(function(filter) {
      sql.where(`${snake(filter.name)} ${filter.operation} ?`, filter.value);
    });
  }

  if (req.query.order) {
    req.query.order.forEach(function(field) {
      sql.orderBy(snake(field.name), field.direction);
    });
  }

  sql.limit(limit);
    
  if (req.query.offset) {
    sql.offset(req.query.offset);
  }

  return sql.toParam();
}

function buildCountQuery(req, tableName) {
  return squel.select()
    .field('COUNT(*) AS count')
    .from(tableName)
    .toParam();
}

function buildFilteredCountQuery(req, tableName) {
  var sql = squel.select()
    .field('COUNT(*) AS count')
    .from(tableName);

  if (req.query.filters) {
    req.query.filters.forEach(function(filter) {
      sql.where(`${snake(filter.name)} ${filter.operation} ?`, filter.value);
    });
  }

  return sql.toParam();
}

function buildQueries(req, tableName) {
  return {
    dataQuery: buildDataQuery(req, tableName),
    countQuery: buildCountQuery(req, tableName),
    filteredCountQuery: buildFilteredCountQuery(req, tableName)
  };
}

function performQuery(db, req, tableName) {
  var {dataQuery, countQuery, filteredCountQuery} = buildQueries(req, tableName);

  return Promise.all([
    db.all(dataQuery.text, dataQuery.values),
    db.get(countQuery.text, countQuery.values),
    db.get(filteredCountQuery.text, filteredCountQuery.values)
  ]).spread(function(data, countResult, filteredCountResult) {
    return {
      data: camelcaseKeys(data, {deep: true}),
      count: countResult.count,
      filteredCount: filteredCountResult.count
    };
  });
}

function buildRoute(db, tableName) {
  return async function(req, res) {
    console.log(JSON.stringify({
      time: new Date(),
      message: 'Incoming request...',
      url: req.url,
      query: req.query,
      ip: req.connection.remoteAddress
    }));

    try {
      var response = await performQuery(db, req, tableName);

      res.send(response);
    } catch (err) {
      res.status(500).send({
        error: err.stack
      });
    }
  };
}

function initDatabase() {
  return sqlite.open(process.env.WORKING_DIRECTORY + '/data/db.sqlite');
}

function initServer(db, tables) {
  var port = process.env.PORT;

  var app = express();

  tables.forEach(function(table) {
    app.get(`/${camelCase(table)}`, buildRoute(db, table));
  });

  app.listen(port);

  console.log(JSON.stringify({
    time: new Date(),
    message: 'Server listening...',
    port: port
  }));
}

(async function() {
  var tables = [
    'cars',
    'motorcycles',
    'boats'
  ];

  var db = await initDatabase();

  initServer(db, tables);
})();

process.on('unhandledRejection', function(err) {
  console.error(err.stack);
  process.exit(1);
});
