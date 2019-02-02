# process-array-elements

Process array of structs using Clojure and Spark. I have json/paruet in below format. 
In this project I coded to read these complex structes to clojure maps(JavaRdd) with out using explode 

```json

{
    "id": 1,
    "stocks": [
                {
                  "name": "Apple",
                  "price": 105.00,
                  "quantity": 50,
                  "stock_dividends": [
                                       {
                                         "amount": 0.53,
                                         "date": "20190213"
                                         },
                                       {
                                         "amount": 0.45,
                                         "date": "20181113"
                                         }
                                       ]
                  },
                {
                  "name": "Ford",
                  "price": 8.05,
                  "quantity": 150,
                  "stock_dividends": [
                                       {
                                         "amount": 0.05,
                                         "date": "20190313"
                                         },
                                       {
                                         "amount": 0.05,
                                         "date": "20181213"
                                         }
                                       ]
                  }
                ]
    }
```

with schema 

````

root
 |-- id: long (nullable = true)
 |-- stocks: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- name: string (nullable = true)
 |    |    |-- price: double (nullable = true)
 |    |    |-- quantity: long (nullable = true)
 |    |    |-- stock_dividends: array (nullable = true)
 |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |-- amount: double (nullable = true)
 |    |    |    |    |-- date: string (nullable = true)
 
````
## Usage


    lein run 
    $ java -jar process-array-elements-0.1.0-standalone.jar


## Examples


## License

Copyright © 2019 FIXME

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
# process-array-elements
