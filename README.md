OLAF-parsel
===========

PostgreSQL uses one CPU core per session. To the best of my knowledge there is no obvious support in PostgreSQL for distributing computation across more cores, when available. This repository offers a PL/pgSQL function that implements a simple parallelisation of SQL queries, so to use as many sessions (hence, potentially, cores of your CPU) as you like.

How does it work? _Divide et impera_: one of the tables that is the subject of the query is split into _n_ chunks, the query is run vs each chunk, and then the results are INSERTed into a results table.

**The parallelisation implemented by this function is feasible only if if your data can be partitioned arbitrarily and the order of the rows is not relevant to your results.** For example, if your query includes grouping vs some column, the partitioning will create duplicate groups in each partition, despite the values being the same! You may not always be able to merge the pieces together (not all operations are associative).

##Usage
```
TEXT parsel(TEXT database_name, TEXT table_to_chunk, TEXT query, TEXT output_table, TEXT table_to_chunk_alias, INTEGER number_of_chunks)
```

where:
```database_name```: the name of the database
```table_to_chunk```: the name of the table that can be split up
```query```: the query to execute in parallel
```output_table```: the table into which results will be inserted
```table_to_chunk_alias``` (optional): if you use an alias for the ```table_to_chunk``` in ```query```, provide it here
```number_of_chunks```: approximate number of processes to split the operation into

##Contacts and acknowledgments
The work is derived from [https://gist.github.com/mjgleaso/8031067](https://gist.github.com/mjgleaso/8031067), last accessed on 29/06/2015. Read the description of the original function by its author on his blog [here](http://geeohspatial.blogspot.com/2013/12/a-simple-function-for-parallel-queries_18.html). I would not have thought of doing this if it wasn't for @mjgleaso, to whom I am very grateful. Note that, differently than the original function, this implementation is independent of referring to some key or another in the split table.

Feel free to contact me at [gc1a13@soton.ac.uk](gc1a13@soton.ac.uk) or [on Twitter](https://twitter.com/giacecco) if you are interested in finding out more.

This work is supported under [SOCIAM: The Theory and Practice of Social Machines](http://sociam.org/). The SOCIAM Project is funded by [the UK Engineering and Physical Sciences Research Council (EPSRC)](https://www.epsrc.ac.uk/) under grant number EP/J017728/1 and comprises the Universities of [Southampton](http://www.southampton.ac.uk/), [Oxford](http://www.ox.ac.uk/) and [Edinburgh](http://www.ed.ac.uk/).

##Licence
The software in this repository is licensed under the [MIT licence](LICENCE.md).

[![CC-BY-SA-4.0](images/ccbysa.png "CC-BY-SA 4.0 logo")](https://creativecommons.org/licenses/by-sa/4.0/) Everything else is licensed under [Creative Commons' CC-BY-SA 4.0 international licence](https://creativecommons.org/licenses/by-sa/4.0/).
