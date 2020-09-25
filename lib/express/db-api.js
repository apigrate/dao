/**
 * @module A set of ExpressJS middleware functions that provide convenient 
 * RESTful API handling.
 * 
 * Note, usage requires the `express` peer dependency. 
 */
const _ = require('lodash');

/**
 * Middleware handling a request to query an entity by id from the database.
 * 
 * Expects:
 * - `res.locals.dbInstructions.dao` property containing the DAO to use for database access.
 * - `res.locals.dbInstructions.id` property containing id of the object to fetch 
 * - `res.locals.dbInstructions.omit` property (optional) containing an array of string property 
 * names to strip/sanitize from the returned object.
 * 
 * @returns calls the next middleware method, after placing the result on `res.locals.result`. 
 * Immediately responds with a HTTP 400 JSON response if parameters are incomplete.
 * If no entity is found, the result will contain an empty object.
 */
async function fetchById(req, res, next) {
  try {
    let dbi = res.locals.dbInstructions;
    if (_.isEmpty(dbi) || !dbi.id) {
      res.status(400).json({message:'Unable to get data.',error:'Missing id.'});
      return;
    }
    let singleEntity = await dbi.dao.get(dbi.id);
    if (_.isEmpty(singleEntity)) {
      res.locals.result = singleEntity;
      next();
      return;
    }
    if(singleEntity && dbi.omit && dbi.omit.length > 0){
      for(let prop in singleEntity){
        if(dbi.omit.indexOf(prop)>=0){
          delete singleEntity[prop]
        }
      }
    }
    res.locals.result = singleEntity;
    next();
    return;
  } catch (ex) {
    next(ex);
  }
}

/**
 * Middleware handling a request to query an entity by a criteria and return a single matching entity. 
 * 
 * Expects:
 * - `res.locals.dbInstructions.dao` property containing the DAO to use for database access.
 * - `res.locals.dbInstructions.query` property containing query object to use
 * 
 * @returns calls the next middleware method, after placing the result on `res.locals.result`. 
 * Immediately responds with a HTTP 400 JSON response if parameters are incomplete.
 * If no entity is found, the result will contain an empty object.
 */
async function fetchOne(req, res, next) {
  try {
    let dbi = res.locals.dbInstructions;
    if (_.isEmpty(dbi) || _.isEmpty(dbi.query)) {
      res.status(400).json({message:'Unable to get data.',error:'Missing query.'});
      return;
    }
    let singleEntity = await dbi.dao.one(dbi.query);
    
    res.locals.result = singleEntity;
    next();
    return;
  } catch (ex) {
    next(ex);
  }
}


/**
 * Middleware handling a request to query entities by a criteria where many entities can be returned.
 *
 * Expects a `res.locals.dbInstructions` entity with the following properties:
 * @property {object} dao the data access object 
 * 
 * @property {object} query query object used to match criteria for a simple implied WHERE clause. Either `query` or `criteria` is required.
 * 
 * @property {object} criteria a criteria helper object for complex queries. Either `query` or `criteria` is required.
 * @property {string} criteria.whereClause a parameterized SQL where clause without the 'WHERE'.
 * @property {string} criteria.parms the parameter array for the whereClause .
 * 
 * @property {object} query_options the query options object
 * @property {array}  query_options.columns (optional) columns to include in the response. If omitted, all are returned.
 * @property {number} query_options.limit the number of rows to limit being returned
 * @property {array}  query_options.orderBy the order by criteria as an array.
 * 
 * @property {boolean} with_total (optional, default false) when true, an additional "total" property is added to the response payload
 * representing the total number of entities in the database that matched the query-regardless of limit parameters.
 *
 * @returns calls the next middleware method, after placing the result on `res.locals.result`. 
 * Immediately responds with a HTTP 400 JSON response if parameters are incomplete.
 */
async function fetchMany(req, res, next) {
  try {
    let dbi = res.locals.dbInstructions;
    if (_.isEmpty(dbi)) {
      res.status(400).json({message:'Unable to get data.',error:'Missing payload.'});
      return;
    }
    if (_.isEmpty(dbi.query) && _.isEmpty(dbi.criteria) ){
      //A "select ALL" is forbidden. A limit criterion must be provided.
      if( _.isEmpty(dbi.query_options)
      || !dbi.query_options.limit
      || (parseInt(dbi.query_options.limit) <= 0 || parseInt(dbi.query_options.limit) > 10000)){
        res.status(400).json({ message:'Unable to get data.',error: "If no query criteria are provided, a valid limit parameter must be provided." });
        return;
      }
      dbi.query = {};
    }

    let result = {};
    if (dbi.query){
      if (dbi.with_total){
        result.total = await dbi.dao.count(dbi.query);
        if(result.total === 0){
          //Don't bother with the full query and return immediately.
          result[dbi.dao.plural] = [];

          res.locals.result = result;
          next();
          return;
        }
      }
      let multipleEntities = await dbi.dao.find(dbi.query, dbi.query_options);
      result[dbi.dao.plural] = multipleEntities;
      
      res.locals.result = result;
      next();
      return;

    } else if (dbi.criteria){
      if (dbi.with_total){
        let temp = await dbi.dao.sqlCommand(`SELECT count(*) AS count FROM ${dbi.dao.table} WHERE ${dbi.criteria.whereClause}`, dbi.criteria.parms);
        result.total = temp[0].count;
        if(result.total === 0){
          //Don't bother with the full query and return immediately.
          result[dbi.dao.plural] = [];
          
          res.locals.result = result;
          next();
          return;
        }
      }
      let multipleEntities = await dbi.dao.selectWhere(dbi.criteria.whereClause, dbi.criteria.parms, dbi.query_options);
      result[dbi.dao.plural] = multipleEntities;
      
      res.locals.result = result;
      next();
      return;
    }
    
    res.status(400).json({ message:'Unable to get data.',error: "Insufficient query criteria were provided." });
    return;
  } catch (ex) {
    next(ex);
  }
}

/**
 * Middleware handling a request to query a count of entities by a criteria.
 * 
 * Expects:
 * - `res.locals.dbInstructions.dao` property containing the DAO to use for database access.
 * - `res.locals.dbInstructions.query` property containing query object to use
 * 
 * @returns calls the next middleware method, after placing the result on `res.locals.result`. 
 * Immediately responds with a HTTP 400 JSON response if parameters are incomplete. 
 */
async function fetchCount(req, res, next) {
  try {
    let dbi = res.locals.dbInstructions;
    if (_.isEmpty(dbi)) {
      res.status(400).json({message:'Unable to get data.',error:'Missing payload.'});
      return;
    }
    let result = await dbi.dao.count(dbi.query);
    res.locals.result = result;
    next();
    return;
  } catch (ex) {
    next(ex);
  }
}

/**
 * Middleware handling a request to create an entity.
 * 
 * Expects:
 * - `res.locals.dbInstructions.dao` property containing the DAO to use for database access.
 * - `res.locals.dbInstructions.toSave` property containing the object to save.
 * @returns calls the next middleware method, after placing the result on `res.locals.result`. 
 * Immediately responds with a HTTP 400 JSON response if parameters are incomplete.
 */
async function create(req, res, next) {
  try {
    let dbi = res.locals.dbInstructions;
    if (_.isEmpty(dbi) || _.isEmpty(dbi.toSave)) {
      res.status(400).json({message:'Unable to create.',error:'Missing payload.'});
      return;
    }
    let result = await dbi.dao.create(dbi.toSave);

    //Support omitting properties from being returned to the consumer.
    if(result && dbi.omit && dbi.omit.length > 0){
      for(let prop in result){
        if(dbi.omit.indexOf(prop)>=0){
          delete result[prop]
        }
      }
    }
    res.locals.result = result;
    next();
    return;
  } catch (ex) {
    next(ex);
  }
}

/**
 * Middleware handling a request to upsert (create or update) an entity.
 * 
 * Expects:
 * - `res.locals.dbInstructions.dao` property containing the DAO to use for database access.
 * - `res.locals.dbInstructions.toSave` property containing the object to save.
 * 
 * @returns calls the next middleware method, after placing the result on `res.locals.result`. 
 * Immediately responds with a HTTP 400 JSON response if parameters are incomplete.
 */
async function save(req, res, next) {
  try {
    let dbi = res.locals.dbInstructions;
    if (_.isEmpty(dbi) || _.isEmpty(dbi.toSave)) {
      res.status(400).json({message:'Unable to save.',error:'Missing payload.'});
      return;
    }
    let result = await dbi.dao.save(dbi.toSave);

    //Support omitting properties from being returned to the consumer.
    if(result && dbi.omit && dbi.omit.length > 0){
      for(let prop in result){
        if(dbi.omit.indexOf(prop)>=0){
          delete result[prop]
        }
      }
    }
    res.locals.result = result;
    next();
    return; 
  } catch (ex) {
    next(ex);
  }
}


/**
 * Middleware handling a request to replace an array of entities with anew
 * array of entities. This operation does NOT delete all old 
 * insert all new. Instead, it determines which existing
 * entries are different 
 * 
 * expected res.locals.dbInstructions:
 * @example {
 *   dao: {}            // the dao you're working with.
 *   query: {}          // the query to find the entities
 *   query_options: {}  // (optional) any query options
 *   toSave: []         // the new array values that should replace the results of the query
 *   comparison: function(v){ return v.product_id}  // comparison function that identifies new vs. existing records (parameter is the value of the array)
 * }
 * @returns calls the next middleware method, after placing the result on `res.locals.result`. 
 * Immediately responds with a HTTP 400 JSON response if parameters are incomplete.
 */
async function saveAll(req, res, next){
  try{
    let dbi = res.locals.dbInstructions;
    if (_.isEmpty(dbi)) {
      res.status(400).json({message:'Unable to save.',error:'Missing payload.'});
      return;
    }
    if (_.isEmpty(dbi.query) &&
      (_.isEmpty(dbi.query_options)
        || _.isEmpty(dbi.query_options.limit)
        || (dbi.query_options.limit * 1 <= 0 || dbi.query_options.limit * 1 > 10000))) {
      res.status(400).json({ message:'Unable to save.',error: "If no query criteria are provided, a valid limit parameter must be provided." });
      return;
    }
    //Get array of existing entities
    let existing = await dbi.dao.find(dbi.query, dbi.query_options);

    //Determine what should be added or removed.
    let shared = _.intersectionBy(existing, dbi.toSave, dbi.comparison );
    let delete_these = _.xorBy(existing, shared, dbi.comparison );
    let create_these = _.xorBy(dbi.toSave, shared, dbi.comparison );

    //Deletes
    console.log(`${delete_these.length} to delete.`)
    let deleted = await Promise.all( delete_these.map( function(entity){ return dbi.dao.deleteOne(entity); } ) );
    //Creates
    console.log(`${create_these.length} to create.`)
    let created = await Promise.all( create_these.map( function(entity){ return dbi.dao.create(entity);    } ) );
    created.forEach((result, idx)=>{
      create_these[idx].id = result.id;
      // no omit support yet since we are only splicing an id, not overwriting the entire result.
    });
    res.locals.result = _.concat(shared, create_these);
    next();
    return; 

  } catch (ex){
    next(ex);
  }
  

}


/**
 * Middleware handling a request to mass-update an array of entities. Each entity must have a primary key id 
 * property present. Entities that do not match existing database rows are ignored.
 * 
 * Expects:
 * a `res.locals.dbInstructions.dao` property containing the DAO to use for database access.
 * a `res.locals.dbInstructions.toSave` property containing the array of objects to save
 * 
 * expected res.locals.dbInstructions:
 * @example {
 *   dao: {}            // the dao you're working with.
 *   toSave: []         // the new array values that should be used to update the entities in the db
 * }
 * @returns calls the next middleware method, after placing the result on `res.locals.result`. 
 * Immediately responds with a HTTP 400 JSON response if parameters are incomplete.
 * @since 4.3.0
 */
async function updateAll(req, res, next){
  try{
    let dbi = res.locals.dbInstructions;
    if (_.isEmpty(dbi) || _.isEmpty(dbi.toSave)) {
      res.status(400).json({message:'Unable to save.',error:'Missing payload.'});
      return;
    }
   
    let updated = await Promise.all( dbi.toSave.map( function(entity){ return dbi.dao.update(entity); } ) );

    res.locals.result = updated;
    next();
    return; 

  } catch (ex){
    next(ex);
  }
}


/**
 * Middleware handling a request to update an entity. (The entity itself must include its db identifier).
 * Expects:
 * - `res.locals.dbInstructions.dao` property containing the DAO to use for database access.
 * - `res.locals.dbInstructions.toUpdate` property containing object data (including the id!) to update.
 * - `res.locals.dbInstructions.omit` property (optional) containing an array of property names to strip/sanitize from the returned object.
 * 
 * @returns calls the next middleware method, after placing the result on `res.locals.result`. 
 * Immediately responds with a HTTP 400 JSON response if parameters are incomplete, or an HTTP 410 if the entity no longer exists.
 */
async function updateById(req, res, next) {
  try {
    let dbi = res.locals.dbInstructions;
    if (_.isEmpty(dbi) || _.isEmpty(dbi.toUpdate)) {
      res.status(400).json({message:'Unable to update.',error:'Missing payload.'});
      return;
    }
    let result = await dbi.dao.update(dbi.toUpdate);
    if (result && result._affectedRows === 0) {
      res.status(410).end();
      return;
    }

    //Support omitting properties from being returned to the consumer.
    if(result && dbi.omit && dbi.omit.length > 0){
      for(let prop in result){
        if(dbi.omit.indexOf(prop)>=0){
          delete result[prop]
        }
      }
    }
    
    res.locals.result = result;
    next();
    return; 
  } catch (ex) {
    next(ex);
  }
}


/**
 * Handle a request to update multiple entities that match a criteria.
 * Expects:
 * - `res.locals.dbInstructions.dao` property containing the DAO to use for database access.
 * - `res.locals.dbInstructions.query` property containing object query to identify the data to update.
 * - `res.locals.dbInstructions.toUpdate` property containing the hash of property data to update.
 * 
 * @returns calls the next middleware method, after placing the result on `res.locals.result`. 
 * Immediately responds with a HTTP 400 JSON response if parameters are incomplete.
 * Note, the result will the `_affectedRows` property with the row count of what was updated (even if _affectedRows = 0).
 */
async function updateMatching(req, res, next) {
  try {
    let dbi = res.locals.dbInstructions;
    if (_.isEmpty(dbi) || _.isEmpty(dbi.toUpdate)) {
      res.status(400).json({message:'Unable to update.',error:'Missing payload.'});
      return;
    }
    if (_.isEmpty(dbi.query)) {
      res.status(400).json({ message:'Unable to update.',error: "Update criteria are required." });
      return;
    }

    let result = await dbi.dao.updateMatching(dbi.toUpdate, dbi.query);
    res.locals.result = result;
    next();
    return; 
  } catch (ex) {
    next(ex);
  }
}


/**
 * Middleware handleing a request to delete an entity by id. 
 * Expects:
 * - `res.locals.dbInstructions.dao` property containing the DAO to use for database access.
 * - `res.locals.dbInstructions.id` property containing the id of the object to delete.
 *  
 * @returns calls the next middleware method, after placing the result on `res.locals.result`. 
 * Immediately responds with a HTTP 400 JSON response if parameters are incomplete.
 * Note, the result will the `_affectedRows` property with the row count of what was updated (even if _affectedRows = 0). 
 */
async function deleteById(req, res, next) {
  try {
    let dbi = res.locals.dbInstructions;
    if (_.isEmpty(dbi) || !dbi.id) {
      res.status(400).json({message:'Unable to delete.',error:'Missing id.'});
      return;
    }
    let result = await dbi.dao.delete(dbi.id);
    if (result && result._affectedRows === 0) {
      res.status(410).end();
      return;
    }
    res.locals.result = result;
    next();
    return; 
  } catch (ex) {
    next(ex);
  }
}

/**
 * Middleware handling a request to delete many entities that match criteria matching the
 * toDelete template entity.
 * Expects:
 * - `res.locals.dbInstructions.dao` property containing the DAO to use for database access.
 * - `res.locals.dbInstructions.toDelete` property containing the query object to use for deleting matching results.
 *  
 * @returns calls the next middleware method, after placing the result on `res.locals.result`. 
 * Immediately responds with a HTTP 400 JSON response if parameters are incomplete, or an HTTP 410 response if no entities were found.
 * Note, the result will the `_affectedRows` property with the row count of what was updated (even if _affectedRows = 0). 
 */
async function deleteMatching(req, res, next) {
  try {
    let dbi = res.locals.dbInstructions;
    if (_.isEmpty(dbi) || _.isEmpty(dbi.toDelete)) {
      res.status(400).json({message:'Unable to delete.',error:'Missing prototype.'});
      return;
    }
    let result = await dbi.dao.deleteMatching(dbi.toDelete);
    if (result && result._affectedRows === 0) {
      res.status(410).end();
      return;
    }
    res.locals.result = result;
    next();
    return; 
  } catch (ex) {
    next(ex);
  }
}

/**
 * Configurable middleware that parses the database query options (i.e. the `limit`, `offset`, `order_by` options typically used for db queries ) on a request.
 * 
 * If a GET or DELETE request, it searches the querystring only.
 * 
 * If a POST or PUT request, it searches the request body only.
 * 
 * @param {array} allowed_query_fields allowable query fields
 * @param {array} default_orderby order by these fields by default (default ['+id'])
 * @param {number} default_limit max records returned (default: 10000)
 * 
 * @returns calls the next middleware method after parsing is completed. Parsing results in the
 * `res.locals.query_options` property set with a hash containing the database query opions (`limit`, `offset`, `order_by`),
 * and `res.locals.modified_query` property set with a hash containing the detected data (according to the `allowed_query_field` parameter)
 * from the the querystring/body, but with the database query options omitted. 
 */
function parseQueryOptions(allowed_query_fields, default_orderby, default_limit){

  function parseQueryOptionsFromObject(obj){
    if(!obj) return null;
    let query = obj;
    let query_options = {
      limit: default_limit || 10000,
      orderBy: default_orderby || ['+id']
    };
    for (key in query) {
      //Valid search fields
      if (allowed_query_fields.indexOf(key) >= 0) continue;
  
      if (key === 'limit') {
        query_options.limit = query[key];
      }
      if (key === 'offset') {
        query_options.offset = query[key];
      }
      if (key === 'order_by') {
        if(_.isArray(query[key])){
          query_options.orderBy = query[key];
        } else {
          query_options.orderBy = query[key].split(',');
        }
        
      }
  
      delete query[key];
    }
    return { modified_query: query, query_options };
  }

  return function(req, res, next){
    let result = null;
    if(['GET','DELETE'].includes(req.method)){
      result = parseQueryOptionsFromObject(req.query);
    } else if (['POST','DELETE'].includes(req.method)){
      result = parseQueryOptionsFromObject(req.body);
    }
    
    if(result){
      res.locals.query_options = result.query_options;
      res.locals.modified_query = result.modified_query;
    }

    next();
  }

}


 
/**
 * Middleware error handler for API requests. If an error reaches this handler, it will be returned with an 
 * HTTP 500 status with a JSON content error.
 * 
 * Addititionally, the handler looks for sqlState property on the error and obfuscates the error message
 * to avoid broadcasting sensitive internal db state information to the API consumer. 
 * 
 * @example <caption>returned response</caption>
 * {
 *   message: "Unexpected error.",
 *   error: errMessage
 * }
 * 
 * @param {object} err the error
 * @param {object} req the request
 * @param {object} res the response
 * @param {function} next (never invoked)
 */
async function handleApiErrors(err, req, res, next){
  console.error(err);
  let errMessage = err.message;
  if(err.sqlState){
    errMessage = 'Database error.';
  }
  res.status(500).json({
    message: "Unexpected error.",
    error: errMessage
  });
}

module.exports.fetchById = fetchById;
module.exports.fetchCount = fetchCount;
module.exports.fetchOne = fetchOne;
module.exports.fetchMany = fetchMany;
module.exports.create = create;
module.exports.updateById = updateById;
module.exports.updateMatching = updateMatching;
module.exports.updateAll = updateAll;
module.exports.save = save;
module.exports.saveAll = saveAll;
module.exports.deleteById = deleteById;
module.exports.deleteMatching = deleteMatching;
module.exports.handleApiErrors = handleApiErrors;
module.exports.parseQueryOptions = parseQueryOptions;
/**
 * A class that encapsulates DB instructions to be used. 
 * by the middleware functions.  
 * 
 * You instantiate DbInstruction in your upstream router methods.
 * Then, add the object to the res.locals property and invoke
 * next() in your router. The downstream api functions
 * will use these instructions to make the appropriate 
 * database query/command and return results consistently. 
 * 
 */
class DbInstructions{
  /**
   * 
   * @param {function} opts.comparison a comparison function used by
   * the `saveAll` middleware function. The comparison identifies 
   * new vs. existing records. It has a single parameter, which is 
   * the value received on the retrieval query or the value of the
   * provided `toSave` parameter.
   * 
   * @param {Dao} opts.dao (required) a Dao object providing data access
   * 
   * @param {any} opts.id identifier for specifying records in the `fetchById`
   * and `deleteById` middleware functions.
   * 
   * @param {array} opts.omit an array of column names to omit from being
   * returned in a query, insert, or update response.
   * 
   * @param {string} query a sql query statement for use in the `fetch*` and 
   * `saveAll` middleware functions
   * 
   * @param {object} query_options a query options object containing 
   * `offset`, `limit`, and/or `orderBy` properties for the query. 
   * 
   * @param {object} toDelete an object to be deleted on a deletion-related
   * middleware functions
   * 
   * @param {object} toUpdate an object to be updated on updated-related
   * middleware functions
   * 
   * @param {object} toSave an object to be inserted (or updated) on create/save-related
   * middleware functions.
   */
  constructor(opts){
    this.comparison = opts ? opts.comparison : null;
    this.dao = opts ? opts.dao : null;
    this.id = opts ? opts.id : null;
    this.omit = opts ? opts.omit : null;
    this.query = opts ? opts.query : null;
    this.query_options = opts ? opts.query_options : null;
    this.toDelete = opts ? opts.toDelete : null;
    this.toSave = opts ? opts.toSave : null;
    this.toUpdate = opts ? opts.toUpdate : null;
  }
}

exports.DbInstructions = DbInstructions;