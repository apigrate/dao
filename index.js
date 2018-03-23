/*
  Copyright 2018 Apigrate, LLC

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/
var _ = require('lodash');
var moment = require('moment');

/**
  Class to provide basic SQL persistence operations.
  @version 2.1.0
  @param {string} table required db table name
  @param {string} entity required logical entity name (singular form)
  @param {object} opts optional options settings to override defaults, shown below
  @example <caption>Default options</caption>
  {
    plural: 'string (derived from English plural rules)',
    created_timestamp_column: 'created',
    updated_timestamp_column: 'updated',
    version_number_column: 'version',
    log_category: 'db'
  }
  @param {object} pool required mysql db pool reference.
  @param {object} logger optional logger instance.
  @return an object to be used for model persistence.
  @example <caption>Note, internal metadata is stored in the the form</caption>
  [{
    column: 'column name',
    sql_type: 'string'
    is_pk: true|false whether a primary key column,
    is_autoincrement: true|false whether it is an autoincrement id
    is_created_timestamp: true|false,
    is_updated_timestamp: true|false,
    is_version: true|false
  }]
*/
function DbEntity(table, entity, opts, pool, logger){
  LOGGER = logger;
  this.pool = pool;
  this.table = table;
  this.entity = entity;

  if(_.isNil(opts)||_.isNil(opts.plural)){
    if(_.endsWith(entity,'y')){
      this.plural = entity.substr(0, entity.lastIndexOf('y')) + 'ies';
    } else if (_.endsWith(entity,'s')) {
      this.plural = entity.substr(0, entity.lastIndexOf('s')) + 'es';
    } else {
      this.plural = entity + 's';
    }
  } else {
    this.plural = opts.plural;
  }

  this.options = (opts || {
    created_timestamp_column: 'created',
    updated_timestamp_column: 'updated',
    version_number_column: 'version',
    log_category: 'db'
  });

  if(logger && LOGGER.info){
    LOGGER = logger;
  } else {
    //use winston
    LOGGER = {error: console.log, warn: console.log, info: function(){}, debug: function(){}, silly: function(){} }
  }

  this.metadata = null;//initialized to empty.
}//constructor

/**
  Promise-returning function that ensures consistent handling of database calls.
*/
DbEntity.prototype.callDb = function(sql, parms){
  var self = this;
  return new Promise(function(resolve, reject){
    self.pool.query(sql, parms, function(err, results, fields){
      if(err){
        reject(err);
      } else {
        resolve(results);
      }
    });
  });
};

/**
  Initializes the internal metadata for further use.
  This is a promise-returning function that must be used for any internal
  method requiring metadata.
*/
DbEntity.prototype.fetchMetadata = function(){
  var self = this;
  return new Promise(function(resolve, reject){
    if(_.isNil(self.metadata)){
      var sql = "SHOW COLUMNS FROM "+ self.table + ";";

      self.callDb(sql, [])
      .then(function(results){
        //LOGGER.silly(self.entity +' fetchMetadata raw results:' + JSON.stringify(results));
        //init the metadata object.
        self.metadata = [];
        _.each(results, function(item){

          var c = {
            column: item.Field,
            sql_type: item.Type,
            pk: item.Key==='PRI',
            nullable: item.Null==='YES',
            default: item.Default,
            autoincrement: item.Extra==='auto_increment',
            is_updated_timestamp: false,
            is_created_timestamp: false,
            is_updated_version: false
          };
          c.is_updated_timestamp=!_.isNil(self.options.updated_timestamp_column)&&c.column===self.options.updated_timestamp_column;
          c.is_created_timestamp=!_.isNil(self.options.created_timestamp_column)&&c.column===self.options.created_timestamp_column;
          c.is_updated_version=!_.isNil(self.options.version_number_column)&&c.column===self.options.version_number_column;

          self.metadata.push(c);
        });

        //LOGGER.silly("Finalized Metadata: "+JSON.stringify(self.metadata));
        resolve(self.metadata);
      })
      .catch(function(err){
        LOGGER.error(self.entity +' fetchMetadata error. Details: ' + err.message);
        reject(err);
      });

    } else {
      //LOGGER.silly("(Metadata already loaded)");
      resolve(self.metadata);
    }
  });
};

/**
  Syntactic sugar for selectOne, selecting a single entity by its PK named 'id'.
  @return promise for entity. If not found, the empty object {} will be returned.
*/
DbEntity.prototype.get = function(id){
  var self = this;
  return new Promise(function(resolve, reject){
    LOGGER.debug(self.entity +' get...');
    var entity = {};

    self.fetchMetadata()
    .then(function(){
      var sql = "SELECT * FROM "+ self.table + " WHERE id = ?";
      LOGGER.debug('  query sql: ' + sql);
      return self.callDb(sql, [id]);
    })
    .then(function(results){
      LOGGER.debug(self.entity +' get result count: ' + results.length);
      LOGGER.silly(self.entity +' get results:' + JSON.stringify(results));
      if(results.length>0){
        entity = results[0];
      }
      resolve(entity);
    })
    .catch(function(err){
      LOGGER.error(self.entity +' get error. Details: ' + err.message);
      reject(err);
    });
  });
};

/**
  Similar to the get function, this function just returns a count (1 or 0) of whether the
  entity exists or not, without retrieving the actual entity, again the PK is
  assumed to be named 'id'.
  @return promise for the count (1 or 0).
*/
DbEntity.prototype.exists = function(id){
  var self = this;
  return new Promise(function(resolve, reject){
    LOGGER.debug(self.entity +' exists...');
    var entity = {};

    self.fetchMetadata()
    .then(function(){
      var sql = "SELECT count(*) as count FROM "+ self.table + " WHERE id = ?";
      LOGGER.debug('  query sql: ' + sql);
      return self.callDb(sql, [id]);
    })
    .then(function(results){
      LOGGER.debug(self.entity +' exists result count: ' + results[0].count);
      LOGGER.silly(self.entity +' exists results:' + JSON.stringify(results));
      resolve(results[0].count);
    })
    .catch(function(err){
      LOGGER.error(self.entity +' exists error. Details: ' + err.message);
      reject(err);
    });
  });
};

/**
  Select all (up to 1000) of a kind of entity.
  @return promise for entity.
  @param opts options to cover orderBy and limit options
  @example
  {
    orderBy: ['+column_name','-column_name'],
    limit: 1000,
    offset: 2500
  }
  @return Promise for an array of objects. If not found, the empty array [] will be returned.
*/
DbEntity.prototype.all = function(opts){
  var self = this;
  return new Promise(function(resolve, reject){
    LOGGER.debug(self.entity +' all...');
    var rs = [];
    self.fetchMetadata()
    .then(function(){
      var sql = "SELECT * FROM "+ self.table + " ";

      sql = self._appendOrderByAndLimit(sql, opts);

      LOGGER.debug('  query sql: ' + sql);

      return self.callDb(sql, []);
    })
    .then(function(results){
      LOGGER.debug(self.entity +' all result count: ' + results.length);
      LOGGER.silly(self.entity +' all results:' + JSON.stringify(results));
      rs = results;
      resolve(rs);
    })
    .catch(function(err){
      LOGGER.error(self.entity +' all error. Details: ' + err.message);
      reject(err);
    });
  });
};

/**
  Performs a query for all rows matching the given template object.
  @param {object} query (optional) 'template' object that is used to match the query.

  All attributes provided on the query object (including those assigned a null
  value) are assumed to be 'ANDed' together.

  If you wish an 'OR' instead, add an opts.booleanMode : 'OR'
  @param {object} opts (optional) query options
  @example
  {
    orderBy: ['+column_name','-column_name'],
    limit: 1000,
    offset: 2500,
    booleanMode: 'OR'
  }
*/
DbEntity.prototype.find = function(query, opts){
  var self = this;
  return new Promise(function(resolve, reject){
    LOGGER.debug(self.entity +' find...');

    self.fetchMetadata()
    .then(function(){
      var sql = "SELECT * FROM "+ self.table + " ";
      var parms = [];
      var bool = ' AND ';
      if(!_.isNil(opts) && !_.isNil(opts.booleanMode)){
        bool=' '+opts.booleanMode+' ';
      }

      var where = '';
      _.each(query, function(v, k){
        if(where!=='') where+=bool;
        where += k+'=?';

        parms.push(v);
      });
      if(where!==''){
        sql+=' WHERE ';
        sql+=where;
      }

      sql = self._appendOrderByAndLimit(sql, opts);

      LOGGER.debug('  query sql: ' + sql);
      LOGGER.debug('  query parms: ' + JSON.stringify(parms));
      return self.callDb(sql, parms);
    })
    .then(function(results){
      LOGGER.debug(self.entity +' find result count: ' + results.length);
      LOGGER.silly(self.entity +' find results: ' + JSON.stringify(results));
      resolve(results);
    })
    .catch(function(err){
      LOGGER.error(self.entity +' find error. Details: ' + err.message);
      reject(err);
    });
  });
};//find

/**
  Similar to the find function. This function counts all rows matching the given
  template object.
  @param {object} query (optional) 'template' object that is used to match the query.

  All attributes provided on the query object (including those assigned a null
  value) are assumed to be 'ANDed' together.

  If you wish an 'OR' instead, add an opts.booleanMode : 'OR'
  @param {object} opts (optional) query options
  @example
  {
    limit: 1000,
    offset: 2500,
    booleanMode: 'OR'
  }
*/
DbEntity.prototype.count = function(query, opts){
  var self = this;
  return new Promise(function(resolve, reject){
    LOGGER.debug(self.entity +' count...');

    self.fetchMetadata()
    .then(function(){
      var sql = "SELECT count(*) as count FROM "+ self.table + " ";
      var parms = [];
      var bool = ' AND ';
      if(!_.isNil(opts) && !_.isNil(opts.booleanMode)){
        bool=' '+opts.booleanMode+' ';
      }

      var where = '';
      _.each(query, function(v, k){
        if(where!=='') where+=bool;
        where += k+'=?';

        parms.push(v);
      });
      if(where!==''){
        sql+=' WHERE ';
        sql+=where;
      }

      sql = self._appendOrderByAndLimit(sql, opts);

      LOGGER.debug('  query sql: ' + sql);
      LOGGER.debug('  query parms: ' + JSON.stringify(parms));
      return self.callDb(sql, parms);
    })
    .then(function(results){
      LOGGER.debug(self.entity +' count result count: ' + results[0].count);
      LOGGER.silly(self.entity +' count results:' + JSON.stringify(results));
      resolve(results[0].count);
    })
    .catch(function(err){
      LOGGER.error(self.entity +' count error. Details: ' + err.message);
      reject(err);
    });
  });
};//find

/**
  Same as the find function, except it returns one row or an empty object if
  nothing is found.
  @param opts {object} query options (not particularly relevant for this function, but available)
  @example
  {
    orderBy: ['+column_name','-column_name'],
    limit: 1000,
    offset: 2500,
    booleanMode: 'OR'
  }
*/
DbEntity.prototype.one = function(query, opts){
  var self = this;
  return new Promise(function(resolve, reject){
    LOGGER.debug(self.entity +' one...');
    var entity = {};
    return self.find(query,opts)
    .then(function(result){
     if(result.length>0){
       entity = result[0];
     }
     resolve(entity);
    })
    .catch(function(err){
     reject(err);
    });
  });

};//one

/**
  Typically used for complex queries or reporting, this function performs a
  sql query on the table backing the entity, selecting anything matching the given WHERE clause
  (do not include the word 'WHERE') and parameters. To avoid SQL injection
  risks, take care to only use this function when user input CANNOT
  affect the WHERE clause being built. It is highly recommended to use
  parameterized SQL.
  @param where {string} parameterized where clause without the 'WHERE'
  @param parms {array} individual data parameters for substitution into the WHERE clause
  @param opts {object} query options (not particularly relevant for this function, but available)
  @example
  {
    orderBy: ['+column_name','-column_name'],
    limit: 1000,
    offset: 2500,
    booleanMode: 'OR'
  }
*/
DbEntity.prototype.selectWhere = function(where, parms, opts){
  var self = this;
  return new Promise(function(resolve, reject){
    LOGGER.debug(self.entity +' selectWhere...');
    var rs = [];
    self.fetchMetadata()
    .then(function(){
      var sql = "SELECT * FROM "+ self.table + " ";

      if(where && where!==''){
        sql+=' WHERE ';
        sql+=where;
      }

      sql = self._appendOrderByAndLimit(sql, opts);

      LOGGER.debug('  query sql: ' + sql);
      LOGGER.debug('  query parms: ' + JSON.stringify(parms));
      return self.callDb(sql, parms);
    })
    .then(function(results){
      LOGGER.debug(self.entity +' selectWhere result count: ' + results.length);
      LOGGER.silly(self.entity +' selectWhere results:' + JSON.stringify(results));
      rs = results;
      resolve(rs);
    })
    .catch(function(err){
      LOGGER.error(self.entity +' selectWhere error. Details: ' + err.message);
      reject(err);
    });
  });
};//selectWhere

/**
  Executes the given generic select statement.
  @param select {string} parameterized select statement (omitting the ORDER BY, LIMIT, and OFFSET,
  which should be provided in the opts parameter).
  @param parms {array} individual data parameters for substitution into the statement
  @param opts {object} query options (not particularly relevant for this function, but available)
  @example
  {
    orderBy: ['+column_name','-column_name'],
    limit: 1000,
    offset: 2500,
    booleanMode: 'OR'
  }
*/
DbEntity.prototype.select = function(select, parms, opts){
  var self = this;
  return new Promise(function(resolve, reject){
    LOGGER.debug(self.entity +' select...');
    var rs = [];
    self.fetchMetadata()
    .then(function(){
      var sql = select;

      sql = self._appendOrderByAndLimit(sql, opts);

      LOGGER.debug('  query sql: ' + sql);
      LOGGER.debug('  query parms: ' + JSON.stringify(parms));
      return self.callDb(sql, parms);
    })
    .then(function(results){
      LOGGER.debug(self.entity +' select result count: ' + results.length);
      LOGGER.silly(self.entity +' select results:' + JSON.stringify(results));
      rs = results;
      resolve(rs);
    })
    .catch(function(err){
      LOGGER.error(self.entity +' select error. Details: ' + err.message);
      reject(err);
    });
  });
};//select

/**
  Creates a single entity.
  @param save object to save. Only the properties provided on this object will
  be included on the INSERT statement (if they match column names).
  @return a promise bearing the save object. It will have its autogenerated key
  field set if one was detected.
*/
DbEntity.prototype.create = function(save, opts){
  var self = this;
  return new Promise(function(resolve, reject){
    LOGGER.debug( self.entity + ' create...' );
    self.fetchMetadata()
    .then(function(){
      var parms = [];
      var cols = '';
      var vals = '';
      var not_ai_pks = _.filter(self.metadata,function(col){
        return (col.pk===true && col.autoincrement===false )||(col.pk===false);
      });

      _.each(save, function(value, property_name){
        //Only props that match columns.
        var col = _.find(not_ai_pks, { column : property_name} );
        if(!_.isNil( col )){
          //Found the column
          if(cols!=='') cols+=', ';
          cols+=col.column;

          if(vals!=='') vals+=', ';
          vals+='?';
          parms.push(_transformToSafeValue(value, col));

        }
      });
      var sql = "INSERT INTO " + self.table + " ("+ cols +") VALUES (" + vals + ");";


      LOGGER.debug('  create sql: ' + sql);
      LOGGER.debug('  create parameters: ' + JSON.stringify(parms));

      return self.callDb(sql, parms);
    })
    .then(function(results){
      LOGGER.silly('  create raw results: ' + JSON.stringify(results));
      //Put the autogenerated id on the entity and return it.
      if(results.affectedRows > 0 && !_.isNil(results.insertId) && results.insertId > 0){
        //console.log('----- id ' + results.insertId)
        var keyCol = _.find(self.metadata, {autoincrement: true, pk: true});
        if(!_.isNil(keyCol)){
          save[keyCol.column] = results.insertId;
        }
        //console.log('----- keycol ' + JSON.stringify(keyCol));

      }
      LOGGER.debug(self.entity +' create results:' + JSON.stringify(results));
      resolve(save);
    })
    .catch(function(err){
      LOGGER.error(self.entity +' create error. Details: ' + err.message);
      reject(err);
    });
  });
};

/**
  Updates a single row by id.
  @param object to save. Only the attributes provided are updated (i.e. performs
  a "sparse" update).
  @return a promise bearing the save object. An _affectedRows attribute will
  be added to this object. Any defaults in the database will
  NOT be included in the returned object, and you should retrieve the object
  again to update their values if you need them.
*/
DbEntity.prototype.update = function(save, opts){
  var self = this;
  return new Promise(function(resolve, reject){
    LOGGER.debug( self.entity + ' update...' );

    self.fetchMetadata().then(function(){
      var parms = [];
      var sql = "UPDATE " + self.table + " SET ";
      var not_pks = _.filter(self.metadata,function(col){ return col.pk===false; });
      var sets = '';

      _.each(save, function(v,k){
        //Exists on not_pks?
        var col = _.find(not_pks, {column: k});
        if(!_.isNil(col)){
          if (
            (self.options && col.column===self.options.created_timestamp_column)
            || (self.options && col.column===self.options.version_number_column)
            || (self.options && col.column===self.options.updated_timestamp_column)){
            //ignore here.
          } else {
            if(sets!=='') sets+=', ';

            sets+=col.column+'=?';

            var parmVal = _transformToSafeValue(v, col);
            if(_.isNil(parmVal)){
              parmVal = null;
            }
            parms.push(parmVal);

          }
        }
      });

      if(sets === ''){
        throw new Error('No data was provided to update.');
      }

      //additional versioning SET clause
      var versioning = '';
      if(self.options.updated_timestamp_column){
        versioning += ', ' + self.options.updated_timestamp_column + '=CURRENT_TIMESTAMP';
      }
      if(self.options.version_number_column){
        versioning += ', ' + self.options.version_number_column + '=' + self.options.version_number_column + '+1';
      }
      sets+=versioning;

      sql+=sets;

      var pks = _.filter(self.metadata,{ pk: true })
      var where = '';
      _.each(pks, function(col){
        if(where!=='') where+=' AND ';
        where+=col.column+'=?';

        var parmVal = save[col.column];
        parms.push(parmVal);
      });
      if(where!==''){
        sql+=' WHERE ';
        sql+=where;
      }

      LOGGER.debug('  update sql: ' + sql);
      LOGGER.debug('  update parameters: ' + JSON.stringify(parms));

      return self.callDb(sql, parms);
    })
    .then(function(results){
      save._affectedRows = results.affectedRows;
      LOGGER.silly(self.entity +' update raw results:' + JSON.stringify(results));
      LOGGER.debug(self.entity +' update results:' + JSON.stringify(save));
      resolve(save);
    })
    .catch(function(err){
      LOGGER.error(self.entity +' update error. Details: ' + err.message);
      reject(err);
    });

  });
};

/**
  Upserts an entity. The save entity is examined for its primary keys and a lookup
  is performed. If the lookup returns a result, an update is made. If the lookup
  returns no results, a create is performed.
  @param object to save. Note only the attributes provided are updated (i.e. performs
  a "sparse" update).
  @return a promise bearing the save results of either the update or create operation.
  If a create was performed, any autogenerated id will be present.
*/
DbEntity.prototype.save = function(save, opts){
  //Get pks.
  var self = this;
  return new Promise(function(resolve, reject){
    LOGGER.debug( self.entity + ' update...' );

    self.fetchMetadata().then(function(){
      var pks = _.filter(self.metadata,{ pk: true });
      //Build PK query.
      var query = {};
      _.each(pks, function(col){
        query[col.column] = save[col.column];
      });

      //Issue the query
      return self.one(query);

    })
    .then(function(oneResult){
      //Perform create or update based on results.
      if(_.isEmpty(oneResult)){
        resolve(self.create(save, opts));
      } else {
        resolve(self.update(save, opts));
      }
    })
    .catch(function(err){
      LOGGER.error(self.entity +' save error. Details: ' + err.message);
      reject(err);
    });
  });//promise
};

/**
  Deletes a single entity by its primary key..
  @param toDelete object whose attributes
  @return a promise bearing the incoming object with an _affectedRows attribute added.
*/
DbEntity.prototype.deleteOne = function(toDelete){
  var self = this;
  return new Promise(function(resolve, reject){

    LOGGER.debug( self.entity + ' delete...' );

    self.fetchMetadata()
    .then(function(){
      var parms = [];
      var sql = "DELETE FROM " + self.table;
      sql+=' WHERE ';
      var pks = _.filter(self.metadata,{ pk: true })
      var where = '';
      _.each(pks, function(col){
        if(where!=='') where+=' AND ';
        where+=col.column+'=?';

        var parmVal = toDelete[col.column];
        parms.push(parmVal);
      });
      if(where===''){
        throw new Error('Could not generate WHERE clause for delete. No primary keys detected.')
      }
      sql+=where;

      LOGGER.debug('  deleteOne sql: ' + sql);
      LOGGER.debug('  deleteOne parameters: ' + JSON.stringify(parms));

      return self.callDb(sql, parms);
    })
    .then(function(results){
      LOGGER.silly(self.entity +' deleteOne raw results:' + JSON.stringify(results));
      toDelete._affectedRows = results.affectedRows;
      LOGGER.debug(self.entity +' deleteOne results:' + JSON.stringify(toDelete));
      resolve(toDelete);
    })
    .catch(function(err){
      LOGGER.error(self.entity +' deleteOne error. Details: ' + err.message);
      reject(err);
    });
  });
};

/**
* Deletes a single entity by its PK named 'id'.
* @return a promise bearing an object with an _affectedRows property.
*/
DbEntity.prototype.delete = function(id){
  var self = this;
  return new Promise(function(resolve, reject){
    LOGGER.debug( self.entity + ' delete...' );
    var entity = {};
    self.fetchMetadata().then(function(){
      var parms = [id];
      var sql = "DELETE FROM " + self.table;
      sql+=' WHERE id = ?';

      LOGGER.debug('  delete sql: ' + sql);
      LOGGER.debug('  delete parameters: ' + JSON.stringify(parms));

      return self.callDb(sql, parms);
    })
    .then(function(results){
      LOGGER.silly(self.entity +' delete raw results:' + JSON.stringify(results));
      entity = {_affectedRows: results.affectedRows};
      LOGGER.debug(self.entity +' delete results:' + JSON.stringify(entity));
      resolve(entity);
    })
    .catch(function(err){
      LOGGER.error(self.entity +' delete error. Details: ' + err.message);
      reject(err);
    });
  });
};

/**
  Deletes entities that match all the given attributes on the criteria object.
  @param criteria object whose attributes specify the conditions for deletion.
  @return a promise bearing the incoming object with an _affectedRows attribute added.
*/
DbEntity.prototype.deleteMatching = function(criteria){
  var self = this;
  return new Promise(function(resolve, reject){
    LOGGER.debug( self.entity + ' delete...' );
    self.fetchMetadata().then(function(){
      var parms = [];
      var sql = "DELETE FROM " + self.table;
      var bool = ' AND ';
      sql+=' WHERE ';
      var where = '';
      _.each(criteria, function(v, k){
        if(where!=='') where+=bool;
        where += k+'=?';

        parms.push(v);
      });
      sql+=where;

      LOGGER.debug('  deleteMatching sql: ' + sql);
      LOGGER.debug('  deleteMatching parameters: ' + JSON.stringify(parms));

      return self.callDb(sql, parms);
    })
    .then(function(results){
      LOGGER.silly(self.entity +' deleteMatching raw results:' + JSON.stringify(results));
      criteria._affectedRows = results.affectedRows;
      LOGGER.debug(self.entity +' deleteMatching results:' + JSON.stringify(criteria));
      resolve(criteria);
    })
    .catch(function(err){
      LOGGER.error(self.entity +' deleteMatching error. Details: ' + err.message);
      reject(err);
    });
  });
};

/**
  Deletes any entity matching the given WHERE clause (do not include the word 'WHERE')
  and parameters. To avoid SQL injection risks, take care to only use this
  function when user input CANNOT affect the WHERE clause being built. It is
  highly recommended to use parameterized SQL.
  @param where where clause without the 'WHERE'
  @param parms parameters for the WHERE clause.
  @return a promise bearing the a simple object with an _affectedRows attribute.
*/
DbEntity.prototype.deleteWhere = function(where, parms){
  var self = this;
  return new Promise(function(resolve, reject){
    LOGGER.debug( self.entity + ' deleteWhere...' );
    var ret = {};
    self.fetchMetadata()
    .then(function(){
      var sql = "DELETE FROM " + self.table;
      sql+=' WHERE ';
      if(_.isNil(where) || where===''){
        throw new Error('Could not delete. A WHERE clause must be provided.');
      }
      sql+=where;

      LOGGER.debug('  deleteWhere sql: ' + sql);
      LOGGER.debug('  deleteWhere parameters: ' + JSON.stringify(parms));

      return self.callDb(sql, parms);
    })
    .then(function(results){
      LOGGER.silly(self.entity +' deleteWhere raw results:' + JSON.stringify(results));
      ret = { _affectedRows : results.affectedRows };
      LOGGER.debug(self.entity +' deleteWhere results:' + JSON.stringify(ret));
      resolve(ret);
    })
    .catch(function(err){
      LOGGER.error(self.entity +' deleteWhere error. Details: ' + err.message);
      reject(err);
    });
  });
};

/**
  Appends the ORDER BY and LIMIT options to a sql statement.
  @param opts options to cover orderBy, limit, and offset options.
  orderBy is an array of column names. Each column name should be immediately
  preceded by + to indicate ascending order, or a - indicating descending order.
  If orderBy is not given explicitly, the results will be returned in ASC order of
  the primary key.
  limit (optional) is the number of rows to be returned. If unspecified, the
  resultset will be limited to 1000 rows.
  offset (optional) is the number of rows to skip from the beginning of the potential
  row resultset if otherwise unlimited. If offset is omitted, the results will
  be taken from the beginning of the resultset.
  @example
  {
    orderBy: ['+column_name','-column_name'],
    limit: 1000,
    offset: 2500
  }
*/
DbEntity.prototype._appendOrderByAndLimit = function(sql, opts){
  var self= this;
  var orderBy = '';
  var limit = '';

  if(!_.isNil(opts)){
    if(!_.isNil(opts.orderBy) && opts.orderBy.length > 0){
      orderBy+=' ORDER BY '
      for(var i=0; i<opts.orderBy.length; i++){
        if(i>0) orderBy+=', '
        var colname = opts.orderBy[i];
        var ord = 'ASC';
        if (_.startsWith(colname, '-')){
          colname = colname.substr(1);
          ord = 'DESC';
        } else if(_.startsWith(colname, '+')) {
          colname = colname.substr(1);
        }
        orderBy+=colname + ' ' + ord;
      }
    }
    if(!_.isNil(opts.limit)){
      limit+=' LIMIT ' + opts.limit;
    } else {
      limit+=' LIMIT 1000'
    }

    if(!_.isNil(opts.offset)){
      limit+=' OFFSET ' + opts.offset;
    }
  }
  if(orderBy === ''){
    var pks = _.filter(self.metadata, {pk: true});
    if(pks.length > 0){
      sql+=' ORDER BY ';
      for(var i=0; i<pks.length; i++){
        if(i>0) orderBy+=', '
        sql+=pks[i].column + ' ASC';
      }
    }
  }
  sql+=orderBy;
  sql+=limit;

  return sql;
}

/**
  Constructs an object for persistence by scraping the attributes from an object
  which match the expected attributes on the data object, and disregarding all
  attributes that are otherwise unexpected. This can be useful when
  controllers are retrieving values from a web form or other source of
  user-provided data.
  @param obj {object} from which to derive the backing entity.
*/
DbEntity.prototype.from = function(obj){
  var self = this;
  return new Promise(function(resolve, reject){
    self.fetchMetadata()
    .then(function(metadata){
      var x = {};
      _.each( metadata, function(meta){
        var v = obj[meta.column];
        if(!_.isUndefined(v) && !_.isArray(v)){
          //Note: explicit nulls will be set on the returned object.
          x[meta.column] = v;
        }
      });
      LOGGER.debug(self.entity +' from results:' + JSON.stringify(x));

      return resolve(x);
    })
    .catch(function(err){
      return reject(err);
    });
  });
}

/**
  Helper that transforms input values to acceptable defaults for database columns.
*/
function _transformToSafeValue(input, column){
  var out = input;
  var datatype = column.sql_type;
  var nullable = column.nullable;
  if( input === '' ){
    //empty string.
    if(datatype==='datetime'|| datatype==='timestamp' ||_.startsWith(datatype, 'int') || _.startsWith(datatype, 'num') || _.startsWith(datatype, 'dec')){
      if(nullable){
        out = null;
      } else {
        throw new Error(column.column + ' is not permitted to be empty.')
      }
    }

  } else if( !_.isNil(input) ) {
    //not null, not undefined
    if(datatype==='datetime'|| datatype==='timestamp'){
      out = moment(input).format('YYYY-MM-DD HH:mm:ss');
    }
  }
  return out;
}

module.exports=DbEntity;
