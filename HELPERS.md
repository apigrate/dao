# Helper Utilities

## criteria
Makes it easier to build a parameterized SQL statement. Useful in
ExpressJS routers to assemble a query from parameters from a request.

### Usage
```javascript
var CriteriaHelper = require('@apigrate/dao/helpers/criteria')

var criteria = new CriteriaHelper({ omitNull: true, omitEmpty: true });
```

Now suppose in an ExpressJS router, you have a request with a querystring
that containing criteria with which you'd like to query the database.
```javascript
router.get('/', function(req, res, next){

  criteria
    .and('column_1', '=', req.query.column_1)
    .and('column_2', '=', req.query.column_2)
    .and('column_3', 'LIKE', req.query.column_3)
    .limit(req.query.limit || 20) // limit to 20 returned
    .offset (req.query.offset || 0 ) // start at beginning
    .orderBy(req.query.orderBy || ['+column_1','-column_5']); //order by these columns


  // criteria.whereClause => 'WHERE column_1=? and column_2=? and column_3 LIKE ?'
  // criteria.parms => [req.query.column_1, req.query.column_2, req.query.column_3]
  // criteria.queryOptions => { limit: 20, offset: 0, orderBy: ['+column_1','-column_5']

  MyTable.selectWhere(
    criteria.whereClause,
    criteria.parms,
    criteria.queryOptions)
    .then(function(logs){
      //render result
    })
    .then(function(logs){
      //handle error
    })

});

```
