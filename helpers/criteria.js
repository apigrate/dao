const _ = require('lodash');

/**
  Convenience utility for chaining together criteria into a where clause
  the database functions that can be used by the mysqlutils operations.
  @param {object} opts example:
  @example {
    omitNull: boolean whether to ignore an and()/or() if passed value is a null, defaults to true]
    omitEmpty: boolean whether to ignore an and()/or() if passed value is a empty (empty string), defaults to true]
  }
  @return {object} example:
  @example
  {
    where: 'WHERE foo=? AND bar=? AND baz=?',
    parms: ['abc', 123, 'def']
  }

*/
function CriteriaHelper(opts){

  this.whereClause='';
  this.parms=[];
  this.queryOptions={};
  this.omitNull=opts && opts.omitIfNull || true;
  this.omitEmpty=opts && opts.omitIfEmpty || true;

};

/**
  Chainable function to inject AND criteria into a where clause. Typically used by controllers
  accepting parameters from the end user.
  @param {string} name column name
  @param {string} oper valid sql operator (=,<,>,<=,>=,LIKE,IS NOT NULL, IS NULL)
  @param {object} the value
*/
CriteriaHelper.prototype.and = function(name, oper, value){
  var self = this;
  return self._where('AND', name, oper, value)
};

/**
  Chainable function to inject OR criteria into a where clause. Typically used by controllers
  accepting parameters from the end user.
  @param {string} name column name
  @param {string} oper valid sql operator (=,<,>,<=,>=,LIKE,IS NOT NULL, IS NULL)
  @param {object} the value
*/
CriteriaHelper.prototype.or = function(name, oper, value){
  var self = this;
  return self._where('OR', name, oper, value)
};

/**
  Chainable function to inject a criteria into where clause. Typically used by controllers
  accepting parameters from the end user.
  @param {string} bool boolean operator AND, OR
  @param {string} name column name
  @param {string} oper valid sql operator (=,<,>,<=,>=,LIKE,IS NOT NULL, IS NULL)
  @param {object} the value
*/
CriteriaHelper.prototype.where = function(bool, name, oper, value){
  var self = this;
  return self._where(bool, name, oper, value)
};

/**
  Chainable function to add an order by criteria. Typically used by controllers
  accepting parameters from the end user.
  @param {array} columns with +- prepended indicating ascending/desceding order
  (e.g. +name equivalent to name ASC, -created equivalent to created DESC)
*/
CriteriaHelper.prototype.orderBy = function(columns){
  var self=this;
  if(_.isEmpty(columns)){
    return self;
  }
  self.queryOptions.orderBy=columns;
  return self;
};

/**
  Chainable function to add a row limit criteria. Typically used by controllers
  accepting parameters from the end user.
  @param {integer} number
*/
CriteriaHelper.prototype.limit = function(number){
  var self=this;
  if(_.isNil(number) || number < 0){
    self.queryOptions.limit=1;
    return self;
  }
  self.queryOptions.limit=Math.floor(number);
  return self;
};

/**
  Chainable function to add a row offset criteria. Typically used by controllers
  accepting parameters from the end user.
  @param {integer} number
*/
CriteriaHelper.prototype.offset = function(number){
  var self=this;
  if(_.isNil(number) || number < 0){
    self.queryOptions.offset=0;
    return self;
  }
  self.queryOptions.offset=Math.floor(number);
  return self;
};

//Internal use
CriteriaHelper.prototype._where = function(bool, name, oper, value){
  var self = this;

  oper = oper.toUpperCase();

  //Some operators are unary
  if(oper!=='IS NOT NULL' && oper!=='IS NULL'){

    if(_.isNil(value) && self.omitNull){
      return self;
    }
    if(isEmpty(value) && self.omitEmpty){
      return self;
    }
  }

  if(self.whereClause===''){
    //Initial WHERE excluded by convention
    self.whereClause += name + ' ' + oper;
  } else {
    self.whereClause += ' ' + bool + ' ' + name  + ' ' + oper;
  }

  if(oper==='IS NOT NULL' || oper==='IS NULL'){
    return self;
  }

  if(oper==='LIKE'){
    self.whereClause += ' ?';//extra space is significant.
    self.parms.push( value );
  } else {
    self.whereClause += '?';
    self.parms.push( value );
  }

  return self;
};

function isEmpty(x){
  return _.isNil(x) || (_.isArray(x) && x.length===0);
  x = x+'';
  return x.trim() === '';
}
module.exports=CriteriaHelper;
