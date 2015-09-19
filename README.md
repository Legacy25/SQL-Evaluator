# SQL Evaluator
A SQL evaluator done as part of a graduate database class

This project translates SQL to an abstract syntax tree (AST) representation and then applies relational algebra rewrites to find an optimum query evaluation strategy. Efficient joins and external sort algorithms enable it to work with huge datasets even on limited memory.

Basic indexing has been used to optmize runtime.

# Build and run

To build, just run the build script -

```
> ./build.sh
```

Do not forget to add executable permission to the script using ```chmod +x```

This will compile all classes in the ```src``` directory and place them in an automatically created ```bin```directory.

To run, you can use the included run script -

```
> ./run.sh
```

# Sample data and queries

Several sample datasets and sql queries are included. These can be found in the ```data``` folder and ```sql`` folders respectively.

There is a set of small sanity check relations ```r, s, t``` comprising of just integer data. A bite-sized TPC-H dataset has been included to test TPC-H queries against. To generate a larger dataset please use a DBGen program for the TPC-H benchmark.

This is one that has been tested and works.
https://github.com/electrum/tpch-dbgen

# Syntax

```
> run.sh --data <path/to/data> [ --swap <path/to/swap> ] <sqlfile1> <sqlfile2> ...
```

The data flag points to the data directory. The swap flag points to a directory used for out of memory operations like external sorts.
