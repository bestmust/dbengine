#include "RelOp.h"
#include "BigQ.h"
#include <unistd.h>
#include <ostream>
#include <fstream>

void* SF_Thread(void *sf_currentObj) {

    SelectFile obj = *((SelectFile *) sf_currentObj);

    obj.Operation();
    pthread_exit(NULL);
    //Peform a getnext to fetch the record from dbfile and keep a counter that increments when a page overflow
}

void SelectFile::Run(DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
    this->sf_inputFile = &inFile;
    this->sf_outputPipe = &outPipe;
    this->sf_selectOperator = &selOp;
    this->sf_literalRecord = &literal;

    int pthreadvar = pthread_create(&thread, NULL, &SF_Thread, (void *) this);
    if (pthreadvar) {
        printf("Error while creating a thread\n");
        exit(-1);
    }

}

void SelectFile::Operation() {

    Record tempRec;

    while (sf_inputFile->GetNext(tempRec, *sf_selectOperator, *sf_literalRecord)) {
        sf_outputPipe->Insert(&tempRec);
    }

    sf_outputPipe->ShutDown();

}

void SelectFile::WaitUntilDone() {
    pthread_join(thread, NULL);
}

void SelectFile::Use_n_Pages(int runlen) {
    runLength = runlen;
}

void* P_Thread(void *p_currentObj) {

    Project obj = *((Project *) p_currentObj);

    obj.Operation();
    pthread_exit(NULL);
    //Peform a getnext to fetch the record from dbfile and keep a counter that increments when a page overflow
}

void Project::Run(Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) {
    this->p_inPipe = &inPipe;
    this->p_outPipe = &outPipe;
    this->p_keepMe = keepMe;
    this->p_numAttsInput = numAttsInput;
    this->p_numAttsOutput = numAttsOutput;

    int pthreadvar = pthread_create(&thread, NULL, &P_Thread, (void *) this);
    if (pthreadvar) {
        printf("Error while creating a thread\n");
        exit(-1);
    }

}

void Project::Operation() {
    Record tempRec;
    while (p_inPipe->Remove(&tempRec)) {
        tempRec.Project(p_keepMe, p_numAttsOutput, p_numAttsInput);
        p_outPipe->Insert(&tempRec);
    }
    p_outPipe->ShutDown();
}

void Project::WaitUntilDone() {
    pthread_join(thread, NULL);
}

void Project::Use_n_Pages(int runlen) {
    runLength = runlen;
}

void* DR_Thread(void *dr_currentObj) {

    DuplicateRemoval obj = *((DuplicateRemoval *) dr_currentObj);

    obj.Operation();
    pthread_exit(NULL);
    //Peform a getnext to fetch the record from dbfile and keep a counter that increments when a page overflow
}

void DuplicateRemoval::Run(Pipe &inPipe, Pipe &outPipe, Schema &mySchema) {
    this->dr_inPipe = &inPipe;
    this->dr_outPipe = &outPipe;
    this->dr_mySchema = &mySchema;

    int pthreadvar = pthread_create(&thread, NULL, &DR_Thread, (void *) this);
    if (pthreadvar) {
        printf("Error while creating a thread\n");
        exit(-1);
    }

}

void DuplicateRemoval::Operation() {

    OrderMaker dr_orderMaker(dr_mySchema);
    Pipe sortedOut(PIPE_BUFF_SIZE);

    //the code in the comment is to create the BigQ thread. but now it is not needed.
    /*
    BigQarg arg={*dr_inPipe,sortedOut,dr_orderMaker,runLength};
    
    pthread_t BigQ_thread;
    
    int error = pthread_create(&BigQ_thread, NULL,BigQThread,(void *) &arg);
        
        if(error)
        {
            cout<<"\n Error while creating the BigQ Thread error# ";
        }
     */
    BigQ bq(*dr_inPipe, sortedOut, dr_orderMaker, runLength);

    Record rec, recNext;
    ComparisonEngine comp;

    if (sortedOut.Remove(&rec)) {
        while (sortedOut.Remove(&recNext)) {
            if (comp.Compare(&rec, &recNext, &dr_orderMaker) != 0) {
                dr_outPipe->Insert(&rec);
                rec = recNext;
            }
        }
        dr_outPipe->Insert(&rec);
    }

    dr_outPipe->ShutDown();

}

void DuplicateRemoval::WaitUntilDone() {
    pthread_join(thread, NULL);
}

void DuplicateRemoval::Use_n_Pages(int runlen) {
    runLength = runlen;
}

void* WO_Thread(void *wo_currentObj) {

    WriteOut obj = *((WriteOut *) wo_currentObj);

    obj.Operation();
    pthread_exit(NULL);
    //Peform a getnext to fetch the record from dbfile and keep a counter that increments when a page overflow
}

void WriteOut::Run(Pipe &inPipe, FILE *outFile, Schema &mySchema) {
    wo_inPipe = &inPipe;
    wo_outFile = outFile;
    wo_mySchema = &mySchema;

    int pthreadvar = pthread_create(&thread, NULL, &WO_Thread, (void *) this);
    if (pthreadvar) {
        printf("Error while creating a thread\n");
        exit(-1);
    }

}

void WriteOut::Operation() {

    Record rec;

    while (wo_inPipe->Remove(&rec)) {
        rec.PrintToFile(wo_outFile, wo_mySchema);
    }

}

void WriteOut::WaitUntilDone() {
    pthread_join(thread, NULL);
}

void WriteOut::Use_n_Pages(int runlen) {
    runLength = runlen;
}

void* SP_Thread(void *sp_currentObj) {

    SelectPipe obj = *((SelectPipe *) sp_currentObj);

    obj.Operation();
    pthread_exit(NULL);
}

void SelectPipe::Run(Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) {
    sp_inPipe = &inPipe;
    sp_outPipe = &outPipe;
    sp_selOp = &selOp;
    sp_literal = &literal;

    int pthreadvar = pthread_create(&thread, NULL, &SP_Thread, (void *) this);
    if (pthreadvar) {
        printf("Error while creating a thread\n");
        exit(-1);
    }

}

void SelectPipe::Operation() {
    Record rec;
    ComparisonEngine comp;

    while (sp_inPipe->Remove(&rec)) {
        if (comp.Compare(&rec, sp_literal, sp_selOp))
            sp_outPipe->Insert(&rec);
    }
    sp_outPipe->ShutDown();
}

void SelectPipe::WaitUntilDone() {
    pthread_join(thread, NULL);
}

void SelectPipe::Use_n_Pages(int runlen) {
    runLength = runlen;
}

void* GB_Thread(void *currentObj) {

    GroupBy obj = *((GroupBy *) currentObj);

    obj.Operation();
    pthread_exit(NULL);
}

void GroupBy::Run(Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) {
    g_inPipe = &inPipe;
    g_outPipe = &outPipe;
    g_groupAtts = &groupAtts;
    g_computeMe = &computeMe;

    int pthreadvar = pthread_create(&thread, NULL, &GB_Thread, (void *) this);
    if (pthreadvar) {
        printf("Error while creating a thread\n");
        exit(-1);
    }

}

void GroupBy::Operation() {

    Pipe sortedOut(PIPE_BUFF_SIZE);

    //start the BigQ to get the sorted output.
    BigQ bq(*g_inPipe, sortedOut, *g_groupAtts, runLength);

    Record rec[2], sumResultRec, outRec;
    ComparisonEngine comp;
    int intResult, finalIntResult = 0, i;
    double doubleResult, finalDoubleResult = 0;

    //first we will create the attsToKeep array of attributes to be used in the merge operation.
    int omAtts = g_groupAtts->numAtts;
    //total attributes in the ordermaker
    int totalAtts = omAtts + 1; //attributes in output

    int attsToKeep[totalAtts]; //used for the merge operation

    attsToKeep[0] = 0; //first attribute is result of function

    // cop[y rest of the attributes from the ordermaker.
    for (i = 0; i < omAtts; i++) {
        attsToKeep[i + 1] = g_groupAtts->whichAtts[i];
                }

    //the type of the result of sum
    Type resultType = Double;

    //remove the first record and apply the function to it and save the sum.
    sortedOut.Remove(&rec[0]);
    resultType = g_computeMe->Apply(rec[0], intResult, doubleResult);
    if (resultType == Int) {
        finalIntResult += intResult;
    } else if (resultType == Double) {
        finalDoubleResult += doubleResult;
                }

    //now remove all the next records compare, apply function and insert the new records.
    i = 1;
    while (sortedOut.Remove(&rec[i % 2])) {

        //if the records are not equal then first merge and insert the second last removed record in output pipe.
        if (comp.Compare(&rec[0], &rec[1], g_groupAtts) != 0) {

            if (resultType == Int) {
                sumResultRec.ComposeRecord(finalIntResult);
            } else if (resultType == Double) {
                sumResultRec.ComposeRecord(finalDoubleResult);
            }
            //merge record with the record that was inserted second last. and insert it into the output queue.
            outRec.MergeRecords(&sumResultRec, &rec[(i + 1) % 2], 1, omAtts, attsToKeep, totalAtts, 1);
            g_outPipe->Insert(&outRec);
            //reinitialize the the final sum results.
            finalIntResult = finalDoubleResult = 0;

        }
        //apply function to newly extracted record.
        g_computeMe->Apply(rec[i % 2], intResult, doubleResult);
        if (resultType == Int) {
            finalIntResult += intResult;
        } else if (resultType == Double) {
            finalDoubleResult += doubleResult;
    }
        i++;
        }

    //insert the last record.
    if (resultType == Int) {
        sumResultRec.ComposeRecord(finalIntResult);
    } else if (resultType == Double) {
        sumResultRec.ComposeRecord(finalDoubleResult);
    }
    //merge record with the record that was inserted second last. and insert it into the output queue.
    //reinitialize the the final sum results.
    outRec.MergeRecords(&sumResultRec, &rec[(i + 1) % 2], 1, omAtts, attsToKeep, totalAtts, 1);
    g_outPipe->Insert(&outRec);

    g_outPipe->ShutDown();

    }

void GroupBy::WaitUntilDone() {
    pthread_join(thread, NULL);
}

void GroupBy::Use_n_Pages(int runlen) {
    runLength = runlen;
}

void* S_Thread(void *currentObj) {

    Sum obj = *((Sum *) currentObj);

    obj.Operation();
    pthread_exit(NULL);
}

void Sum::Run(Pipe &inPipe, Pipe &outPipe, Function &computeMe) {
    s_inPipe = &inPipe;
    s_outPipe = &outPipe;
    s_computeMe = &computeMe;

    int pthreadvar = pthread_create(&thread, NULL, &S_Thread, (void *) this);
    if (pthreadvar) {
        printf("Error while creating a thread\n");
        exit(-1);
    }

}

void Sum::Operation() {

    Record rec, outRec;
    Type ret = Double; // Initialize to atleast print 0 and not give an error
    int intResult, finalIntResult = 0;
    double doubleResult, finalDoubleResult = 0;

    //efficiently fine out the sum and save it.
    if (s_inPipe->Remove(&rec)) {
        ret = s_computeMe->Apply(rec, intResult, doubleResult);
        if (ret == Int) {
            finalIntResult += intResult;
            while (s_inPipe->Remove(&rec)) {
                s_computeMe->Apply(rec, intResult, doubleResult);
                finalIntResult += intResult;
            }
        } else if (ret == Double) {
            finalDoubleResult += doubleResult;
            while (s_inPipe->Remove(&rec)) {
                s_computeMe->Apply(rec, intResult, doubleResult);
                finalDoubleResult += doubleResult;
            }
        } else {
            cout << "Wront type returned by Apply function in Function. Error called from Sum::Operation";
        }
    }
    /*
     * The following code in comment is obsolete. But it is kept to demonstrate how simple it is to compose record
     * without creating new schema, directly from the integer/double values.
     * 
    //create a new schema. and then try to create the new record.
    stringstream strResultStream;
    string str, out;
    char *name = "tempSumSchema";
    
    outRec.bits =new (std::nothrow) char[sizeof(double)];
    
        ofstream fout(name);
        fout << "BEGIN" << endl;
        fout << "SUM_table" << endl;
        fout << "sum.tbl" << endl;
        fout << "SUM ";
        if (ret == Int) {
                fout << "Int" << endl;
        } else {
                fout << "Double" << endl;
        }
        fout << "END";
        fout.close();
        Schema mySchema("tempSumSchema", "SUM_table");
        if (ret == Int) {
                strResultStream << finalIntResult;
                out = strResultStream.str();
                str.append(out);
                str.append("|");
        } else {
                strResultStream << finalDoubleResult;
                out = strResultStream.str();
                str.append(out);
                str.append("|");
        }
        const char*src = str.c_str();
        outRec.ComposeRecord(&mySchema, src);
        remove(name);
     */

    //Compuse a new record based on the integer or double result.
    if (ret == Int) {
        outRec.ComposeRecord(finalIntResult);
    } else if (ret == Double) {
        outRec.ComposeRecord(finalDoubleResult);
    }

    //insert the new record in the output pipe and 
    s_outPipe->Insert(&outRec);
    s_outPipe->ShutDown();

}

void Sum::WaitUntilDone() {
    pthread_join(thread, NULL);
}

void Sum::Use_n_Pages(int runlen) {
    runLength = runlen;
}

void* J_Thread(void *currentObj) {

    Join obj = *((Join *) currentObj);

    obj.Operation();
    pthread_exit(NULL);
}

void Join::Run(Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) {
    j_inPipeL = &inPipeL;
    j_inPipeR = &inPipeR;
    j_outPipe = &outPipe;
    j_selOp = &selOp;
    j_literal = &literal;

    int pthreadvar = pthread_create(&thread, NULL, &J_Thread, (void *) this);
    if (pthreadvar) {
        printf("Error while creating a thread\n");
        exit(-1);
    }
}

void Join::Operation() {
    // Create ordermaker for left and right input pipes using getsortorders
    // If OrderMaker has been created Use BigQ to sort both pipes
    // Merge the two pipes using merge function
    // If Ordermaker has not been created (not an equality check then use block nested loop join


    //parameters for sort-merge join
    OrderMaker *om_Left = new OrderMaker;
    OrderMaker *om_Right = new OrderMaker;
    Pipe *bigQLeftSorted = new Pipe(10000000);
    Pipe *bigQRightSorted = new Pipe(10000000);

    Record *lRecord, *rRecord, *templeft, *tempright, record;
    vector<Record *> recordVectorLeft;
    vector<Record *> recordVectorRight;
    vector<Record*>::iterator itrLeft;
    vector<Record*>::iterator itrRight;
    bool blnFirst = true;
    int leftisEmpty = 0, rightisEmpty = 0, templeftflag = 0, temprightflag = 0;
    //this flag when set indicates that the pipes are empty
    int nomoreleftrecs = 0, nomorerightrecs = 0, setleft = 0, setright = 0;

    //parameters for block-nested join
    DBFile dbfile;
    Record leftRecord, rightRecord;
    Page page;
    int flagtoDetermineEndofAddRecord = 0;
    vector<Record *> recordVector;
    vector<Record*>::iterator itr;
    Record *tmpRecord;

    //parameters for comparison
    ComparisonEngine compEng;
    int result = 0, resultofComparison = 0;

    //parameters for generating the schema
    int numAttsLeft = 0, numAttsRight = 0;
    int *attsToKeep;
    int numAttsToKeep = 0, startOfRight = 0, i;

    //get the numAtts for both left input Pipe and right input Pipe using the Schema
    numAttsLeft = 7;
    numAttsRight = 5;
    int size = numAttsLeft + numAttsRight;
    attsToKeep = (int *) malloc(size * sizeof (int));
    for (i = 0; i < numAttsLeft; i++) {
        attsToKeep[i] = i;
        numAttsToKeep++;
    }
    startOfRight = i;
    for (int k = 0, j = numAttsLeft; k < numAttsRight; k++, j++) {
        attsToKeep[j] = k;
        numAttsToKeep++;
    }

    //JOIN OPERATION BEGINS
    int ret_OM = this->j_selOp->GetSortOrders(*om_Left, *om_Right);
    if (ret_OM != 0) {
        //SORT MERGE JOIN
        BigQ jn_bqLeft(*(this->j_inPipeL), *(bigQLeftSorted), *om_Left,
                this->runLength);
        //sleep(1);
        BigQ jn_bqRight(*(this->j_inPipeR), *(bigQRightSorted), *om_Right,
                this->runLength);

        while (true) {
            if (blnFirst) {
                //fill the left vector untill first non-matching records
                while (true) {
                    lRecord = new Record;
                    //break the while loop when no more records in the left pipe
                    if (!bigQLeftSorted->Remove(lRecord)) {
                        nomoreleftrecs = 1;
                        break;
                    } else {
                        if (templeftflag == 0) {
                            templeft = new Record;
                            templeft = lRecord;
                            recordVectorLeft.push_back(lRecord);
                            templeftflag = 1;
                        } else {
                            result = compEng.Compare(lRecord, om_Left,
                                    templeft, om_Left);
                            if (result == 0) {
                                recordVectorLeft.push_back(lRecord);
                            } else {
                                break;
                            }
                        }
                    }
                }

                //fill the right vector untill first non-matching records
                while (true) {
                    rRecord = new Record;
                    //break the while loop when no more records in the left pipe
                    if (!bigQRightSorted->Remove(rRecord)) {
                        nomorerightrecs = 1;
                        break;
                    } else {
                        if (temprightflag == 0) {
                            //just store the first record in the temporarily to compare it with the other records while filling the record vector
                            tempright = new Record;
                            tempright = rRecord;
                            recordVectorRight.push_back(rRecord);
                            temprightflag = 1;
                        } else {
                            result = compEng.Compare(rRecord, om_Right,
                                    tempright, om_Right);
                            if (result == 0) {
                                recordVectorRight.push_back(rRecord);
                            } else {
                                break;
                            }
                        }
                    }
                }
                blnFirst = false;
            }
            templeft = new Record();
            tempright = new Record();
            if (!recordVectorLeft.empty()) {
                templeft = recordVectorLeft.front();
            } else
                leftisEmpty = 1;

            if (!recordVectorRight.empty()) {
                tempright = recordVectorRight.front();
            } else
                rightisEmpty = 1;

            if ((leftisEmpty == 0) && (rightisEmpty == 0)) {
                result
                        = compEng.Compare(templeft, om_Left, tempright,
                        om_Right);
            } else {
                break;
            }

            //now check for conditions when the remaining records in recordvector are greater than the records in the other pipe which stil has records
            if ((nomoreleftrecs == 1) && result > 0) {
                setright = 1;
            }

            if ((nomorerightrecs == 1) && result < 0) {
                setleft = 1;
            }

            if (result == 0) {
                // Step 1:now compare each and every record in left vector to each and every record in right vector
                //            merge them and insert into outputPipe
                // Step 2:empty out both the record vectors once all records are done
                // Step 3: insert fresh records into the both the pipes
                for (itrLeft = recordVectorLeft.begin(); itrLeft
                        != recordVectorLeft.end(); itrLeft++) {
                    for (itrRight = recordVectorRight.begin(); itrRight
                            != recordVectorRight.end(); itrRight++) {
                        resultofComparison = compEng.Compare(*itrLeft, *itrRight, j_literal, j_selOp);
                        if (resultofComparison == 1) {
                            record.MergeRecords(*itrLeft, *itrRight,
                                    numAttsLeft, numAttsRight, attsToKeep,
                                    numAttsToKeep, startOfRight);
                            (this->j_outPipe)->Insert(&record);
                        }
                    }
                }
                recordVectorLeft.clear();
                recordVectorRight.clear();

                //if nomore records in either of the pipes than you can safely exit the while loops
                if ((nomoreleftrecs == 1) || (nomorerightrecs == 1)) {
                    if (nomoreleftrecs == 1)
                        delete lRecord;
                    if (nomorerightrecs == 1)
                        delete rRecord;
                    break;
                }

                //since the last remove of both the pipes was not pushed into the vector we insert it now and than delete those record pointer

                recordVectorLeft.push_back(lRecord);
                templeft = new Record;

                templeft = lRecord;

                recordVectorRight.push_back(rRecord);
                tempright = new Record;
                tempright = rRecord;

                //fill the left vector untill first non-matching records
                while (true) {
                    lRecord = new Record;
                    //break the while loop when no more records in the left pipe
                    if (!bigQLeftSorted->Remove(lRecord)) {
                        nomoreleftrecs = 1;
                        break;
                    } else {
                        result = compEng.Compare(lRecord, om_Left, templeft,
                                om_Left);
                        if (result == 0) {
                            recordVectorLeft.push_back(lRecord);
                        } else {
                            break;
                        }
                    }
                }

                //fill the right vector untill first non-matching records
                while (true) {
                    rRecord = new Record;
                    //break the while loop when no more records in the left pipe
                    if (!bigQRightSorted->Remove(rRecord)) {
                        nomorerightrecs = 1;
                        break;
                    } else {
                        result = compEng.Compare(rRecord, om_Right, tempright,
                                om_Right);
                        if (result == 0) {
                            recordVectorRight.push_back(rRecord);
                        } else {
                            break;
                        }
                    }
                }
            }//case 2 when left is less that right
            else if (result < 0) {
                //Step1: if nomore records in either of the pipes than you can safely exit the while loops
                if (setleft == 0) {
                    if ((nomoreleftrecs == 1) || (nomorerightrecs == 1)) {
                        if (nomoreleftrecs == 1)
                            delete lRecord;
                        if (nomorerightrecs == 1)
                            delete rRecord;
                        break;
                    }
                } else
                    setleft = 0;

                //Step2: empty out the records from left vector and fill them with new records untill the first non matching record you get
                recordVectorLeft.clear();

                //Step3: push the record of left pipe only as in right pipe  there are still records and than delete those record pointers
                recordVectorLeft.push_back(lRecord);
                templeft = new Record;
                templeft = lRecord;

                //Step4: fill the left vector untill first non-matching records
                while (true) {
                    lRecord = new Record;
                    //break the while loop when no more records in the left pipe
                    if (!bigQLeftSorted->Remove(lRecord)) {
                        nomoreleftrecs = 1;
                        break;
                    } else {
                        result = compEng.Compare(lRecord, om_Left, templeft,
                                om_Left);
                        if (result == 0) {
                            recordVectorLeft.push_back(lRecord);
                        } else {
                            break;
                        }
                    }
                }
            }//case 3 when right is greater than left
            else {
                //Step1: if nomore records in either of the pipes than you can safely exit the while loops
                if (setright == 0) {
                    if ((nomoreleftrecs == 1) || (nomorerightrecs == 1)) {
                        if (nomoreleftrecs == 1)
                            delete lRecord;
                        if (nomorerightrecs == 1)
                            delete rRecord;
                        break;
                    }
                } else
                    setright = 0;

                //Step2: empty out the records from right vector and fill them with new records untill the first non matching record you get
                recordVectorRight.clear();

                //Step3: push the record of right pipe only as in left pipe  there are still records and than delete those record pointers
                recordVectorRight.push_back(rRecord);
                tempright = new Record;
                tempright = rRecord;

                //Step4: fill the left vector untill first non-matching records
                while (true) {
                    rRecord = new Record;
                    //break the while loop when no more records in the left pipe
                    if (!bigQRightSorted->Remove(rRecord)) {
                        nomorerightrecs = 1;
                        break;
                    } else {
                        result = compEng.Compare(rRecord, om_Right, tempright,
                                om_Right);
                        if (result == 0) {
                            recordVectorRight.push_back(rRecord);
                        } else {
                            break;
                        }
                    }
                }
            }
            leftisEmpty = 0;
            rightisEmpty = 0;
        }
        bigQLeftSorted->ShutDown();
        bigQRightSorted->ShutDown();
        (this->j_outPipe)->ShutDown();

    }//BLOCK NESTED LOOP JOIN BEGINS
    else {
        //First insert the records of right pipe into the temp dbfile
        dbfile.Create("dbfile.bin", heap, NULL);
        while ((this->j_inPipeR)->Remove(&rightRecord)) {
            dbfile.Add(rightRecord);
        }
        dbfile.Close();
        //Now first load the records of left pipe into the page in memory and for each record in page scan through the dbfile to
        //to obtain a matching record.if record matches than merge them else continue
        dbfile.Open("dbfile.bin");
        int counter = 0;
        while (true) {
            //insert the records into the recordVector
            if ((this->j_inPipeL)->Remove(&leftRecord) && (counter < 5000)) {
                recordVector.push_back(&leftRecord);
                counter++;
            } else {
                //Case1
                //if the counter becomes equal to 3000 than compare each record in dbfile with every record in record vector
                if (counter == 5000) {
                    counter = 0;
                    dbfile.MoveFirst();
                    while (dbfile.GetNext(rightRecord) == 1) {
                        for (itr = recordVector.begin(); itr
                                != recordVector.end(); itr++) {
                            resultofComparison = compEng.Compare(*itr, &rightRecord, j_literal, j_selOp);
                            if (resultofComparison == 1) {
                                record.MergeRecords(*itr, &rightRecord,
                                        numAttsLeft, numAttsRight, attsToKeep,
                                        numAttsToKeep, startOfRight);
                                (this->j_outPipe)->Insert(&record);
                            }
                        }
                    }
                    recordVector.clear();
                    recordVector.push_back(&leftRecord);
                    counter++;
                }//Case2
                    //In this case the counter was less than 5000 but the pipe becomes empty
                else {
                    dbfile.MoveFirst();
                    while (dbfile.GetNext(rightRecord) == 1) {
                        for (itr = recordVector.begin(); itr
                                != recordVector.end(); itr++) {
                            resultofComparison = compEng.Compare(*itr, &rightRecord, j_literal, j_selOp);
                            if (resultofComparison == 1) {
                                record.MergeRecords(*itr, &rightRecord,
                                        numAttsLeft, numAttsRight, attsToKeep,
                                        numAttsToKeep, startOfRight);
                                (this->j_outPipe)->Insert(&record);
                            }
                        }
                    }
                    recordVector.clear();
                    remove("dbfile.bin");
                    remove("dbfile.bin.meta");
                    dbfile.Close();
                    break;
                }
            }
        }

    }
    (this->j_outPipe)->ShutDown();
}

void Join::WaitUntilDone() {
    pthread_join(thread, NULL);
}

void Join::Use_n_Pages(int runlen) {
    runLength = runlen;
}