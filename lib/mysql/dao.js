/*
  Copyright 2018-2024 Apigrate, LLC

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
var debug = require('debug')('gr8:db');
var verbose = require('debug')('gr8:db:verbose');

/**
  Class to provide basic SQL persistence operations.
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
class Dao{
  constructor (table, entity, opts, pool){ 
    this.pool = pool;
    this.table = table;
    this.entity = entity;

    if(!opts||!opts.plural){
      if(entity.endsWith('ey')){
        this.plural = entity + 's';
      } else if(entity.endsWith('y')){
        this.plural = entity.substr(0, entity.lastIndexOf('y')) + 'ies';
      } else if (entity.endsWith('s')) {
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
      version_number_column: 'version'
    });

    this.metadata = null;//initialized to empty.
  }//constructor


  /**
   * Executes a parameterized SQL command (SELECT, INSERT, UPDATE, DELETE, etc.)
   * @param {string} sql parameterized SQL command
   * @param {array} parms parameters
   */
  async sqlCommand(sql, parms){
    let self = this;
    
    return new Promise(function(resolve, reject){
      if(!sql.includes("SHOW COLUMNS")){ //suppress metadata logging.
        debug('  sql: ' + sql);
        debug('  parms: ' + JSON.stringify(parms));
      }

      self.pool.query(sql, parms, function(err, result, fields){
        if(err){
          reject(err);
          return;
        }
        if(!sql.includes("SHOW COLUMNS")){
          verbose(self.entity +' raw result: %o', result);
        }
        resolve(result);
      });
    });
  };

  /**
   * Deprecated method. Use the `sqlCommand` method instead!
   * @deprecated
   */
  async callDb(sql, parms){ return this.sqlCommand(sql, parms); }

  /**
    Initializes the internal metadata for further use.
    This is a promise-returning function that must be used for any internal
    method requiring metadata.
  */
  async loadMetadata(){
    try{
      if(!this.metadata){
        var sql = "SHOW COLUMNS FROM "+ this.table + ";";

        let results = await this.sqlCommand(sql, [])
        //init the metadata object.
        this.metadata = results.map(item => {
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
          c.is_updated_timestamp = typeof this.options.updated_timestamp_column !== 'undefined' 
            && this.options.updated_timestamp_column !== null 
            && c.column===this.options.updated_timestamp_column; 

          c.is_created_timestamp = typeof this.options.created_timestamp_column !== 'undefined' 
            && this.options.created_timestamp_column !== null 
            && c.column===this.options.created_timestamp_column;
          
          c.is_updated_version = typeof this.options.version_number_column !== 'undefined' 
            && this.options.version_number_column !== null 
            && c.column===this.options.version_number_column; 

          return c;
        });

      } else{
        return this.metadata;
      }
    }catch(ex){
      console.error(`Error. ${ex.message}`);
    }

  }

  /**
    Syntactic sugar for selectOne, selecting a single entity by its PK named 'id'.
    @return promised entity or NULL if not found.
  */
  async get(id, opts){
    
    try{
      debug(this.entity +' get...');
      var entityResult = null;

      await this.loadMetadata()
      
      var whichcols = opts && opts.columns ? opts.columns.join(',') : '*';
      var sql = "SELECT "+whichcols+" FROM "+ this.table + " WHERE id = ?";
      let results = await this.sqlCommand(sql, [id]);
      
      debug(this.entity +' get result count: ' + results.length);
      if(results.length>0){
        entityResult = results[0];
      }
      return entityResult;
      
    } catch (err){
      console.error(`${this.entity} get error. Details: ${err.message}`);
      throw err;
    }
  };

  /**
    Similar to the get function, this function just returns a count (1 or 0) of whether the
    entity exists or not, without retrieving the actual entity, again the PK is
    assumed to be named 'id'.
    @return promise for the count (1 or 0).
  */
  async exists(id){
    try {
      debug(this.entity +' exists...');
      await this.loadMetadata()

      var sql = "SELECT count(*) as count FROM "+ this.table + " WHERE id = ?";
      let results = await this.sqlCommand(sql, [id]);
  
      debug(this.entity +' exists result count: ' + results[0].count);
      return results[0].count;

    } catch (err){
      console.error(`${this.entity} exists error. Details: ${err.message}`);
      throw err;
    }
  }


  /**
    Select all of a kind of entity, applying no filters.
    By default, up to 1000 entities will be returned. If you need more than 1000,
    adjust this by using the `opts.limit` property.
    @param opts options to cover orderBy and limit options
    @example
    {
      columns: ['column1', 'column2', 'column3'], //to be returned
      orderBy: ['+column_name','-column_name'],
      limit: 1000,
      offset: 2500
    }
    @return Promise for an array of objects. If not found, the empty array [] will be returned.
  */
  async all(opts){
    try {
      debug(this.entity +' all...');
      var rs = [];
      await this.loadMetadata();
     
      var whichcols = opts && opts.columns ? opts.columns.join(',') : '*';
      var sql = "SELECT "+whichcols+" FROM "+ this.table + " ";

      sql = this._appendOrderByAndLimit(sql, opts);

      return await this.sqlCommand(sql, []);
     
    } catch (err){
      console.error(`${this.entity} all error. Details: ${err.message}`);
      throw err;
    }
  }


  /**
    Performs a query for all rows matching the given template object.
    @param {object} query (optional) 'template' object that is used to match the query.

    All attributes provided on the query object (including those assigned a null
    value) are assumed to be 'ANDed' together.

    If you wish an 'OR' instead, add an opts.booleanMode : 'OR'
    @param {object} opts (optional) query options
    @example
    {
      columns: ['column1', 'column2', 'column3'], //to be returned
      orderBy: ['+column_name','-column_name'],
      limit: 1000,
      offset: 2500,
      booleanMode: 'OR'
    }
  */
  async query(query, opts){
    try{
      debug(this.entity +' query...');

      await this.loadMetadata();
  
      var whichcols = opts && opts.columns ? opts.columns.join(',') : '*';
      var sql = "SELECT "+whichcols+" FROM "+ this.table + " ";
      var parms = [];
      var bool = ' AND ';
      if(opts && typeof opts.booleanMode !== 'undefined' && opts.booleanMode !== null){
        bool=' '+opts.booleanMode+' ';
      }

      var where = '';
      for( let k in query ){
        if(where!=='') where+=bool;
        where += k+'=?';

        parms.push(query[k]);
      }
      if(where!==''){
        sql+=' WHERE ';
        sql+=where;
      }

      sql = this._appendOrderByAndLimit(sql, opts);

      let results = await this.sqlCommand(sql, parms);
      debug(this.entity +' query result count: ' + results.length);
      return results;
    
    } catch (err){
      console.error(`${this.entity} query error. Details: ${err.message}`);
      throw err;
    }
  }//query

  /**
   * @deprecated use query(query, opts) instead.
   */
  async filter(query, opts){
    return this.query(query, opts);
  }


  /**
    Similar to the filter function. This function counts all rows matching the given
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
    @returns {Promise<number>} the number of rows matching the query
  */
  async count(query, opts){
    try {
      debug(this.entity +' count...');

      await this.loadMetadata();

      var sql = "SELECT count(*) as count FROM "+ this.table + " ";
      var parms = [];
      var bool = ' AND ';
      if(opts && typeof opts.booleanMode !== 'undefined' && opts.booleanMode !== null){
        bool=' '+opts.booleanMode+' ';
      }

      var where = '';
      for(let k in query){
        if(where!=='') where+=bool;
        where += k+'=?';

        parms.push(query[k]);
      }
      if(where!==''){
        sql+=' WHERE ';
        sql+=where;
      }

      sql = this._appendOrderByAndLimit(sql, opts);

      let results = await this.sqlCommand(sql, parms);
    
      debug(this.entity +' count result count: ' + results[0].count);
      return results[0].count;

    } catch (err){
      console.error(`${this.entity} count error. Details: ${err.message}`);
      throw err;
    }
  }//count


  /**
    Same as the find function, except it returns one row or NULL if
    nothing is found.
    @param opts {object} query options (not particularly relevant for this function, but available)
    @example
    {
      columns: ['column1', 'column2', 'column3'], //to be returned
      orderBy: ['+column_name','-column_name'],
      limit: 1000,
      offset: 2500,
      booleanMode: 'OR'
    }
  */
  async one(query, opts){
    try {
      debug(this.entity +' one...');
      var entityResult = null;
      let result = await this.filter(query,opts)
      if(result.length>0){
        entityResult = result[0];
      }
      return entityResult;
    } catch (err){
      console.error(`${this.entity} one error. Details: ${err.message}`);
      throw err;
    }
  } //one


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
      columns: ['column1', 'column2', 'column3'], //to be returned
      orderBy: ['+column_name','-column_name'],
      limit: 1000,
      offset: 2500,
      booleanMode: 'OR'
    }
    @returns the array of results.
  */
  async selectWhere(where, parms, opts){
    try {
      debug(this.entity +' selectWhere...');

      await this.loadMetadata();
   
      var whichcols = opts && opts.columns ? opts.columns.join(',') : '*';
      var sql = "SELECT "+whichcols+" FROM "+ this.table + " ";

      if(where && where!==''){
        sql+=' WHERE ';
        sql+=where;
      }

      sql = this._appendOrderByAndLimit(sql, opts);

      let results = await this.sqlCommand(sql, parms);
    
      debug(this.entity +' selectWhere result count: ' + results.length);
      
      return results;
    } catch (err){
      console.error(`${this.entity} selectWhere error. Details: ${err.message}`);
      throw err;
    }
  } //selectWhere


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
  async select(select, parms, opts){
    try {
      debug(this.entity +' select...');
      
      await this.loadMetadata();
      
      var sql = select;

      sql = this._appendOrderByAndLimit(sql, opts);
      let results = await this.sqlCommand(sql, parms);
    
      debug(this.entity +' select result count: ' + results.length);
      return results;
        
    } catch (err){
      console.error(`${this.entity} select error. Details: ${err.message}`);
      throw err;
    }
  } //select

  /**
    Creates a single entity.
    @param {object} save object to save. Only the properties provided on this object will
    be included on the INSERT statement (if they match column names).
    @param opts with additional options:
    @param {boolean} opts.explicit_pk When true, any pk properties present
    on the save entity will be added to the INSERT statement (n). Defaults to false.  (since 4.4.0)
    @return a promise bearing the save object. It will have its autogenerated key
    field set if one was detected.
  */
  async create(save, opts){
    try {
      debug( this.entity + ' create...' );
      await this.loadMetadata()

      var parms = [];
      var cols = '';
      var vals = '';
      var not_ai_pks = this.metadata.filter((col)=>{
        return (col.pk===true && col.autoincrement===false )||(col.pk===false);
      });
      if(opts && opts.explicit_pk){
        not_ai_pks = this.metadata;//everything is eligible
      }

      for(let property_name in save){ 
        //Only props that match columns.
        var col = not_ai_pks.find( x => x.column === property_name );
        if( col ){
          //Found the column
          if(cols!=='') cols+=', ';
          cols+=col.column;

          if(vals!=='') vals+=', ';
          vals+='?';
          parms.push(this._transformToSafeValue(save[property_name], col));
        }
      }
      var sql = "INSERT INTO " + this.table + " ("+ cols +") VALUES (" + vals + ");";

      let results = await this.sqlCommand(sql, parms);

      verbose('  create raw results: ' + JSON.stringify(results));
      //Put the autogenerated id on the entity and return it.
      if(results.affectedRows > 0 && results.insertId && results.insertId > 0){
        //console.log('----- id ' + results.insertId)
        var keyCol = this.metadata.find(x => x.autoincrement === true && x.pk === true);
        if( keyCol ){
          save[keyCol.column] = results.insertId;
        }
        //console.log('----- keycol ' + JSON.stringify(keyCol));

      }
      debug(this.entity +' create results:' + JSON.stringify(results));
      return save;

    } catch (err){
      console.error(`${this.entity} create error. Details: ${err.message}`);
      throw err;
    }
  } // create


  /**
    Updates a single row by id.
    @param object to save. Only the attributes provided are updated (i.e. performs
    a "sparse" update).
    @return a promise bearing the save object. An _affectedRows attribute will
    be added to this object. Any defaults in the database will
    NOT be included in the returned object, and you should retrieve the object
    again to update their values if you need them.
  */
  async update(save, opts){
    try {
      debug( this.entity + ' update...' );

      await this.loadMetadata();

      var parms = [];
      var sql = "UPDATE " + this.table + " SET ";
      var not_pks = this.metadata.filter((col) => { return col.pk===false; });
      var sets = '';

      for(let k in save){
        //Exists on not_pks?
        var col = not_pks.find( x => x.column === k);
        if( col ){
          if (
            (this.options && col.column===this.options.created_timestamp_column)
            || (this.options && col.column===this.options.version_number_column)
            || (this.options && col.column===this.options.updated_timestamp_column)){
            //ignore here.
          } else {
            if(sets!=='') sets+=', ';

            sets+=col.column+'=?';

            var parmVal = this._transformToSafeValue(save[k], col);
            if(typeof parmVal==='undefined' || parmVal === null){
              parmVal = null;
            }
            parms.push(parmVal);

          }
        }
      }

      if(sets === ''){
        throw new Error('No data was provided to update.');
      }

      //additional versioning SET clause
      var versioning = '';
      if(this.options.updated_timestamp_column){
        versioning += ', ' + this.options.updated_timestamp_column + '=CURRENT_TIMESTAMP';
      }
      if(this.options.version_number_column){
        versioning += ', ' + this.options.version_number_column + '=' + this.options.version_number_column + '+1';
      }
      sets+=versioning;

      sql+=sets;

      var pks = this.metadata.filter( x => x.pk === true );
      var where = '';

      for(let col of pks){
        if(where!=='') where+=' AND ';
        where+=col.column+'=?';

        var parmVal = save[col.column];
        parms.push(parmVal);
      }
      if(where!==''){
        sql+=' WHERE ';
        sql+=where;
      }

      let results =  await this.sqlCommand(sql, parms);

      save._affectedRows = results.affectedRows;
      verbose(this.entity +' update raw results:' + JSON.stringify(results));
      debug(this.entity +' update results:' + JSON.stringify(save));
      return save;
  
    } catch (err){
      console.error(`${this.entity} update error. Details: ${err.message}`);
      throw err;
    }
  } // update


  /**
    Updates a number of rows that match the given filter object (ANDed together).
    @param save (required) to save. Only the attributes provided are updated (i.e. performs
    a "sparse" update). IMPORTANT NOTE: primary keys on this object will NOT be updated 
    (they will be ignored). If you need to update primary keys, use the `sqlCommand` method.
    @param criteria (required) the filter object used to build the WHERE clause identifying objects
    that should be updated.
    @param opts (optional) options specifying the limit and order by parameters to further specify 
    what gets updated.
    @return a promise bearing the save object. An _affectedRows attribute will
    be added to this object. Any defaults in the database will
    NOT be included in the returned object, and you should retrieve the object
    again to update their values if you need them.
    @throws an error if criteria is omitted or empty (entire table updates are not permitted via this method).
  */
  async updateMatching(save, criteria, opts){
    try {
      if(!criteria || Object.keys(criteria)===0) throw new Error(`Entire table updates are not permitted.`);
      debug( this.entity + ' update...' );

      await this.loadMetadata();

      var parms = [];
      var sql = "UPDATE " + this.table + " SET ";
      var bool = ' AND ';
      var not_pks =this.metadata.filter(col => col.pk === false);
      var sets = '';

      for(let k in save){
        //Exists on not_pks?
        var col = not_pks.find(x => x.column === k);
        if( col ){
          if (
            (this.options && col.column===this.options.created_timestamp_column)
            || (this.options && col.column===this.options.version_number_column)
            || (this.options && col.column===this.options.updated_timestamp_column)){
            //ignore here.
          } else {
            if(sets!=='') sets+=', ';

            sets+=col.column+'=?';

            var parmVal = this._transformToSafeValue(save[k], col);
            if(typeof parmVal === 'undefined' || parmVal === null){
              parmVal = null;
            }
            parms.push(parmVal);

          }
        }
      }

      if(sets === ''){
        throw new Error('No data was provided to update.');
      }

      //additional versioning SET clause
      var versioning = '';
      if(this.options.updated_timestamp_column){
        versioning += ', ' + this.options.updated_timestamp_column + '=CURRENT_TIMESTAMP';
      }
      if(this.options.version_number_column){
        versioning += ', ' + this.options.version_number_column + '=' + this.options.version_number_column + '+1';
      }
      sets+=versioning;

      sql+=sets;

      var where = '';
      for(let k in criteria){
        let v = criteria[k];
        if(where!=='') where+=bool;
        where += k+'=?';

        parms.push(v);
      }
      if(where!==''){
        sql+=' WHERE ';
        sql+=where;
      }

      sql = this._appendOrderByAndLimit(sql, opts);

      let results =  await this.sqlCommand(sql, parms);

      save._affectedRows = results.affectedRows;
      verbose(this.entity +' update raw results:' + JSON.stringify(results));
      debug(this.entity +' update results:' + JSON.stringify(save));
      return save;

    } catch (err){
      console.error(`${this.entity} update error. Details: ${err.message}`);
      throw err;
    }
  } // updateMatching


  /**
    Upserts an entity. The save entity is examined for its primary keys and a lookup
    is performed. If the lookup returns a result, an update is made. If the lookup
    returns no results, a create is performed.
    @param object to save. Note only the attributes provided are updated (i.e. performs
    a "sparse" update).
    @return a promise bearing the save results of either the update or create operation.
    If a create was performed, any autogenerated id will be present.
  */
  async save(save, opts){
    try {
      debug( this.entity + ' update...' );

      await this.loadMetadata();
      var pks = this.metadata.filter(x => x.pk === true );
      //Build PK query.
      var query = {};
      for(let col of pks){
        query[col.column] = save[col.column];
      }

      //Issue the query
      let oneResult = await this.one(query);
    
      //Perform create or update based on results.
      if(!oneResult){
        return this.create(save, opts);
      } else {
        return this.update(save, opts);
      }

    } catch (err){
      console.error(`${this.entity} save error. Details: ${err.message}`);
      throw err;
    }
  } // save


  /**
    Deletes a single entity by its primary key..
    @param toDelete object whose attributes
    @return a promise bearing the incoming object with an _affectedRows attribute added.
  */
  async deleteOne(toDelete){
    try {

      debug( this.entity + ' delete...' );

      await this.loadMetadata();
      
      var parms = [];
      var sql = "DELETE FROM " + this.table;
      sql+=' WHERE ';
      var pks = this.metadata.filter(x => x.pk === true);
      var where = '';
      for(let col of pks){
        if(where!=='') where+=' AND ';
        where+=col.column+'=?';

        var parmVal = toDelete[col.column];
        parms.push(parmVal);
      }
      if(where===''){
        throw new Error('Could not generate WHERE clause for delete. No primary keys detected.')
      }
      sql+=where;

      let results = await this.sqlCommand(sql, parms);
    
      verbose(this.entity +' deleteOne raw results:' + JSON.stringify(results));
      toDelete._affectedRows = results.affectedRows;
      debug(this.entity +' deleteOne results:' + JSON.stringify(toDelete));
      return toDelete;

    } catch (err){
      console.error(`${this.entity} deleteOne error. Details: ${err.message}`);
      throw err;
    }
  } // deleteOne


  /**
  * Deletes a single entity by its PK named 'id'.
  * @return a promise bearing an object with an _affectedRows property.
  */
  async delete(id){
    try {
      debug( this.entity + ' delete...' );
      var entity = {};
      await this.loadMetadata();
      var parms = [id];
      var sql = "DELETE FROM " + this.table;
      sql+=' WHERE id = ?';

      let results = await this.sqlCommand(sql, parms);
    
      verbose(this.entity +' delete raw results:' + JSON.stringify(results));
      entity = {_affectedRows: results.affectedRows};
      debug(this.entity +' delete results:' + JSON.stringify(entity));
      return entity;
    
    } catch (err){
      console.error(`${this.entity} delete error. Details: ${err.message}`);
      throw err;
    }
  } // delete (by id)


  /**
    Deletes entities that match all the given attributes on the criteria object.
    @param criteria (required) object whose attributes specify the conditions for deletion.
    @return a promise bearing the incoming object with an _affectedRows attribute added.
    @throws an error if criteria is omitted or empty (entire table deletion is not permitted via this method).
  */
  async deleteMatching(criteria){
    try {
      if(!criteria || Object.keys(criteria).length === 0) throw new Error(`Entire table deletes are not permitted.`);
      debug( this.entity + ' delete...' );
      await this.loadMetadata();
      var parms = [];
      var sql = "DELETE FROM " + this.table;
      var bool = ' AND ';
      sql+=' WHERE ';
      var where = '';
 
      for(let k in criteria){
        if(where!=='') where+=bool;
        where += k+'=?';

        parms.push(criteria[k]);
      }
      sql+=where;

      let results = await this.sqlCommand(sql, parms);
    
      verbose(this.entity +' deleteMatching raw results:' + JSON.stringify(results));
      criteria._affectedRows = results.affectedRows;
      debug(this.entity +' deleteMatching results:' + JSON.stringify(criteria));
      return criteria;

    } catch (err){
      console.error(`${this.entity} deleteMatching error. Details: ${err.message}`);
      throw err;
    }
  } // deleteMatching


  /**
    Deletes any entity matching the given WHERE clause (do not include the word 'WHERE')
    and parameters. To avoid SQL injection risks, take care to only use this
    function when user input CANNOT affect the WHERE clause being built. It is
    highly recommended to use parameterized SQL.
    @param where where clause without the 'WHERE'
    @param parms parameters for the WHERE clause.
    @return a promise bearing the a simple object with an _affectedRows attribute.
  */
  async deleteWhere(where, parms){
    try {
      debug( this.entity + ' deleteWhere...' );
      var ret = {};
      await this.loadMetadata();
      var sql = "DELETE FROM " + this.table;
      sql+=' WHERE ';
      if( !where ){
        throw new Error('Could not delete. A WHERE clause must be provided.');
      }
      sql+=where;

      let results = await this.sqlCommand(sql, parms);
    
      verbose(this.entity +' deleteWhere raw results:' + JSON.stringify(results));
      ret = { _affectedRows : results.affectedRows };
      debug(this.entity +' deleteWhere results:' + JSON.stringify(ret));
      return ret;
    } catch (err){
      console.error(`${this.entity} deleteWhere error. Details: ${err.message}`);
      throw err;
    }
  } // deleteWhere 

  /**
    (Synchronous) Appends the ORDER BY and LIMIT options to a sql statement.
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
  _appendOrderByAndLimit(sql, opts){
    var orderBy = '';
    var limit = '';

    if(opts){
      if(opts.orderBy && opts.orderBy.length > 0){
        orderBy+=' ORDER BY '
        for(var i=0; i<opts.orderBy.length; i++){
          if(i>0) orderBy+=', '
          var colname = opts.orderBy[i];
          var ord = 'ASC';
          if (colname.startsWith('-')){
            colname = colname.substr(1);
            ord = 'DESC';
          } else if(colname.startsWith('+')) {
            colname = colname.substr(1);
          }
          orderBy+=colname + ' ' + ord;
        }
      }
      if(opts.limit){
        limit+=' LIMIT ' + opts.limit;
      } else {
        limit+=' LIMIT 1000'
      }

      if(opts.offset){
        limit+=' OFFSET ' + opts.offset;
      }
    }
    if(orderBy === ''){
      var pks = this.metadata.filter(x => x.pk === true );
      if(pks.length > 0){
        orderBy+=' ORDER BY ';
        for(var i=0; i<pks.length; i++){
          if(i>0) orderBy+=', '
          orderBy+=pks[i].column + ' ASC';
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
  async from(obj){
    await this.loadMetadata();
    var x = {};
    this.metadata.forEach(meta => {
      let v = obj[meta.column];
      if(typeof v !== 'undefined' && !Array.isArray(v)){
        //Note: explicit nulls will be set on the returned object.
        x[meta.column] = v;
      }
    });
    debug(this.entity +' from results:' + JSON.stringify(x));
    return x;
  }

  /**
    (Synchronous) Helper that transforms input values to acceptable defaults for database columns.
  */
  _transformToSafeValue(input, column){
    var out = input;
    var datatype = column.sql_type;
    var nullable = column.nullable;
    if( input === '' ){
      //empty string.
      if(datatype==='datetime'|| datatype==='timestamp' || datatype.startsWith('int') || datatype.startsWith('num') || datatype.startsWith('dec')){
        if(nullable){
          out = null;
        } else {
          throw new Error(column.column + ' is not permitted to be empty.')
        }
      }

    }
    return out;
  }

}//class

/** @returns {Dao} */
exports.Dao = Dao;