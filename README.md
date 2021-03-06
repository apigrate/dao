# @apigrate/dao
A library that simplifies working with relational database databases, using a Data Access Object (DAO) pattern.
It provides promise-based functions making it easy to get objects out of database table rows with intuitive language.

# What it does.
Create a DAO for each table in your database. Once instantiated, you can use any of the available methods outlined below
 to query, create, update, and delete rows from that table.
> Note, this library is currently designed to work with mysql databases (note the peer dependency).
> Support for additional databases may become available in the future.

## Single Row Queries

* __get__ - selects a single row by id
* __exists__ - similar to __get__, but returns a 1 if found or 0 if not found.

## Multiple Row Queries

* __all__ - selects all rows in a table (offset and limit are supported for paging)
* __find__ - selects rows that meet criteria
* __count__ - similar to __find__, but returns a count of the rows that match the criteria
* __one__ - selects and returns only *one* of a list of rows that meet criteria
* __selectWhere__ - same as __find__, but an explicit where clause is used as input.
* __select__ - supports a fully parameterized SQL select statement

## Insert and Update
* __create__ - inserts a row in a table (returns an autogenerated id if applicable)
* __update__ - updates a row in a table by primary key (supports sparse updates)
* __save__ - "upserts" a row in a table (i.e. performs an update if an primary keys match an existing row, else performs an insert)

## Delete

* __delete__ - delete a single row by its id
* __deleteOne__ - same as delete, but supports multi-column primary keys
* __deleteMatching__ - deletes anything that matches the provided criteria
* __deleteWhere__ - deletes anything that matches the provided WHERE clause

## Generic
* __sqlCommand__ - issues any kind of parameterized SQL command.

# How to use it.

## Instantiate

__Important Prerequsite__: your app should configure a [mysql connection pool](https://www.npmjs.com/package/mysql#pooling-connections) that it can pass to this library. This library is not opinionated about connection management. It does not close or otherwise manage pool connections directly.


```javascript
//var pool = (assumed to be provided by your app)
const {Dao} = require('@apigrate/dao');

//An optional configuration object containing some options that you might want to use on a table.  

var opts = {
  created_timestamp_column: 'created',
  updated_timestamp_column: 'updated',
  version_number_column: 'version'
};

var Customer = new Dao('t_customer', 'customer', opts, pool);
//Note, in addition to tables, you use this on views as well...
```

## Read/Query

### Get by id.
Get a single table row by id and return it as an object. Returns `null` when not found.
```javascript
//Get a customer by id = 27
let result = await Customer.get(27);
//result --> {id: 27, name: 'John Smith', city: 'Chicago', active: true ... }
```

### Query

#### Count

Simplest form of query. Retrieves a count rows from DB matching the filter object.

```javascript
//Search for customers where status='active' and city='Chicago'
let result = await Customer.count({status: 'active', city: 'Chicago'})
//result --> 2 
```

#### Filter

Simple filter-matches-all query. Retrieves all rows from DB matching the filter object as an array. Returns an empty array when not found.

```javascript
//Search for customers where status='active' and city='Chicago'
let result = await Customer.filter({status: 'active', city: 'Chicago'})
//result --> [ {id: 27, name: 'John Smith', city: 'Chicago' active: true ... }, {id: 28, name: 'Sally Woo', city: 'Chicago', active: true ... }, ...]
```

#### One

Identical to Filter, except only the first entity from results is returned as an object. Returns `null` when not found.

```javascript
//Search for customers where status='active' and city='Chicago'
let result = await Customer.one({status: 'active', city: 'Chicago'})
//result --> {id: 27, name: 'John Smith', city: 'Chicago' active: true ... }
```

### Advanced Query

Select multiple entities matching a where clause and parameters.

```javascript
//Retrieve active customers in Chicago, Indianpolis.
let result = await Customer.selectWhere("active=? AND (city=? or city=?)"  [true, "Chicago", "Indianapolis"]); 
//result --> [ {id: 27, name: 'John Smith', city: 'Chicago' active: true ... }, {id: 28, name: 'Sally Woo', city: 'Chicago', active: true ... }, {id: 28, name: 'Jake Plumber', city: 'Indianapolis', active: true ... }, ...]
```

## Create

Creates a new entity.

```javascript
//Create a new customer
let customerToSave = { name: 'Acme, Inc.', city: 'Chicago', active: true}; 
let result = await Customer.create(customerToSave); 
//result --> {id: 27, name: 'Acme, Inc.', city: 'Chicago', active: true}; (assuming id is auto-generated)
```

## Update

Updates an entity by primary key (which must be included on the payload).

```javascript
//Update an existing customer by id.
let customerToSave = {id: 27, name: 'Acme, Inc.', city: 'Chicago', active: true};
customerToSave.active = false;
let result = await Customer.update(customerToSave); 
//result --> {id: 27, name: 'Acme, Inc.', city: 'Chicago', active: false, _affectedRows: 1};
```

## Delete

### Delete by ID

Deletes an entity by primary key.

```javascript
//Delete customer id = 27
let result = await Customer.delete(27); 
//result --> {_affectedRows: 1, ...}
```

### Delete Matching

Deletes multiple entities matching the filter object.

```javascript
//Delete inactive customers in Chicago
let result = await Customer.deleteMatching({active: false, city: "Chicago"}); 
//result --> {_affectedRows: 3, active: false, city: "Chicago"}
```

### Advanced Delete

Deletes multiple entities matching a where clause and parameters.

```javascript
//Delete inactive customers in Chicago, Indianpolis.
let result = await Customer.deleteWhere("active=? AND (city=? or city=?)"  [false, "Chicago", "Indianapolis"]); 
//result --> {_affectedRows: 4}
```

## Generic SQL Support

Use the `sqlCommand` method to issue any kind of parameterized SQL command (SELECT, INSERT, UPDATE, DELETE, etc.). The result
returned is simply the result returned from the underlying [mysql](https://www.npmjs.com/package/mysql) library callback function.

```javascript
//Custom query example
let result = await Customer.sqlCommand("SELECT id, name from my_customer_view where active=? AND (city=? or city=?)"  [false, "Chicago", "Indianapolis"]); 
//result --> [{id: 27, name: "Acme, Inc."}, {id: 33, name: "American Finance Corporation"}, {id: 35, name: "Integrity Engineering"}]
```


### Support for Logging
The [debug](https://www.npmjs.org/debug) library is used. Use `process.env.NODE_ENV='gr8:db'` for general debugging. For verbose logging (outputs raw responses on create, update, delete operations) use `gr8:db:verbose`.

Note: as of version 3.x logger injection is no longer supported and will be ignored.
#### What gets logged?
1. error messages (database exceptions) are logged to `console.error`
4. at `DEBUG='gr8:db'`, the following is logged:
   * method call announcement
   * SQL used for query/execution
   * a count of the results (if any).
5. at `DEBUG='gr8:db:verbose'`, the following is logged:
   * raw SQL command output from the underlying mysql library create, update, and delete statements.
