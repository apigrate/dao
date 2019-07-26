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
class CriteriaHelper{
  constructor(opts){
    this.whereClause='';
    this.parms=[];
    this.queryOptions={};
    this.omitNull=opts && opts.omitIfNull || true;
    this.omitEmpty=opts && opts.omitIfEmpty || true;
  }


  /**
    Chainable function to inject AND criteria into a where clause. Typically used by controllers
    accepting parameters from the end user.
    @param {string} name column name
    @param {string} oper valid sql operator (=,<,>,<=,>=,LIKE,IS NOT NULL, IS NULL)
    @param {object} the value
  */
  and(name, oper, value){

    return this._where('AND', name, oper, value)
  }

  /**
    Chainable function to inject OR criteria into a where clause. Typically used by controllers
    accepting parameters from the end user.
    @param {string} name column name
    @param {string} oper valid sql operator (=,<,>,<=,>=,LIKE,IS NOT NULL, IS NULL)
    @param {object} the value
  */
  or(name, oper, value){

    return this._where('OR', name, oper, value)
  }

  /**
    Chainable function to inject a criteria into where clause. Typically used by controllers
    accepting parameters from the end user.
    @param {string} bool boolean operator AND, OR
    @param {string} name column name
    @param {string} oper valid sql operator (=,<,>,<=,>=,LIKE,IS NOT NULL, IS NULL)
    @param {object} the value
  */
  where(bool, name, oper, value){

    return this._where(bool, name, oper, value)
  }

  /**
    Chainable function to add an order by criteria. Typically used by controllers
    accepting parameters from the end user.
    @param {array} columns with +- prepended indicating ascending/desceding order
    (e.g. +name equivalent to name ASC, -created equivalent to created DESC)
  */
  orderBy(columns){

    if(this.isEmpty(columns)){
      return this;
    }
    this.queryOptions.orderBy=columns;
    return this;
  }

  /**
    Chainable function to add a row limit criteria. Typically used by controllers
    accepting parameters from the end user.
    @param {integer} number
  */
  limit(number){

    if(_.isNil(number) || number < 0){
      this.queryOptions.limit=1;
      return this;
    }
    this.queryOptions.limit=Math.floor(number);
    return this;
  }

  /**
    Chainable function to add a row offset criteria. Typically used by controllers
    accepting parameters from the end user.
    @param {integer} number
  */
  offset(number){

    if(_.isNil(number) || number < 0){
      this.queryOptions.offset=0;
      return this;
    }
    this.queryOptions.offset=Math.floor(number);
    return this;
  }

  //Internal use
  _where(bool, name, oper, value){

    oper = oper.toUpperCase();

    //Some operators are unary
    if(oper!=='IS NOT NULL' && oper!=='IS NULL'){

      if(_.isNil(value) && this.omitNull){
        return this;
      }
      if(this.isEmpty(value) && this.omitEmpty){
        return this;
      }
    }

    if(this.whereClause===''){
      //Initial WHERE excluded by convention
      this.whereClause += name + ' ' + oper;
    } else {
      this.whereClause += ' ' + bool + ' ' + name  + ' ' + oper;
    }

    if(oper==='IS NOT NULL' || oper==='IS NULL'){
      return this;
    }

    if(oper==='LIKE'){
      this.whereClause += ' ?';//extra space is significant.
      this.parms.push( value );
    } else {
      this.whereClause += '?';
      this.parms.push( value );
    }

    return this;
  }

  isEmpty(x){
    return _.isNil(x) || (!_.isArray(x) && x.length===0);
    x = x+'';
    return x.trim() === '';
  }
}
module.exports=CriteriaHelper;
