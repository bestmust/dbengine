/* 
 * File:   Heap.h
 * Author: mustaqim
 *
 * Created on April 4, 2013, 1:21 AM
 */

#ifndef HEAP_H
#define	HEAP_H

#include "Pipe.h"
#include "BigQ.h"
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Heap.h"
#include "DBFile.h"
#include <iostream>
#include <stdio.h>
#include <vector>
#include<cstdlib>
#include<time.h>
#include <algorithm>

class HeapDBFile: public GenericDBFile {

public:

    HeapDBFile();

    //Creates a new DBFile
    //fpath- path at which the file is to be placed.
    //filetype - either heap, sorted, tree   This will decide the implementation
    //returns -1 if file was not created.
    int Create(char *fpath, fType file_type, void *startup);

    //Opens an existing file
    int Open(char *fpath);
    int Close();

    void Load(Schema &myschema, char *loadpath);

    void MoveFirst();
    void Add(Record &addme);
    int GetNext(Record &fetchme);
    int GetNext(Record &fetchme, CNF &cnf, Record &literal);

};


#endif	/* HEAP_H */

