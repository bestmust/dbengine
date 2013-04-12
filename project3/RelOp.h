#ifndef REL_OP_H
#define REL_OP_H

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"

class RelationalOp {
protected:
    pthread_t thread;
public:
    // blocks the caller until the particular relational operator 
    // has run to completion
    virtual void WaitUntilDone() = 0;

    virtual void Operation() = 0; // make it =0; after all functions are implemented

    virtual void Run() {}; // make it =0; after all functions are implemented

    virtual void Use_n_Pages(int n) = 0;
};

class SelectFile : public RelationalOp {
private:
    DBFile *sf_inputFile;
    Pipe *sf_outputPipe;
    CNF *sf_selectOperator;
    Record *sf_literalRecord;
    int runLength;

public:

    void Run(DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
    void Operation();
    void WaitUntilDone();
    void Use_n_Pages(int n);

};

class SelectPipe : public RelationalOp {
private:
    Pipe *sp_inPipe, *sp_outPipe;
    CNF *sp_selOp;
    Record *sp_literal;
    int runLength;
public:
    void Run(Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
    void WaitUntilDone();
    void Operation();
    void Use_n_Pages(int n);
};

class Project : public RelationalOp {
private:
    Pipe *p_inPipe, *p_outPipe;
    int *p_keepMe, p_numAttsInput, p_numAttsOutput;
    int runLength;

public:
    void Operation();
    void Run(Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);
    void WaitUntilDone();
    void Use_n_Pages(int n);
};

class Join : public RelationalOp {
private:
    Pipe *j_inPipeL, *j_inPipeR, *j_outPipe;
    CNF *j_selOp;
    Record *j_literal;
    int runLength;
public:
    void Run(Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal);
    void WaitUntilDone();
    void Use_n_Pages(int n);
    void Operation();
};

class DuplicateRemoval : public RelationalOp {
private:
    Pipe *dr_inPipe, *dr_outPipe;
    Schema *dr_mySchema;
    int runLength;
public:
    void Run(Pipe &inPipe, Pipe &outPipe, Schema &mySchema);
    void WaitUntilDone();
    void Use_n_Pages(int n);
    void Operation();
};

class Sum : public RelationalOp {
private:
    Pipe *s_inPipe, *s_outPipe;
    Function *s_computeMe;
    int runLength;
public:
    void Run(Pipe &inPipe, Pipe &outPipe, Function &computeMe);
    void WaitUntilDone();
    void Use_n_Pages(int n);
    void Operation();
};

class GroupBy : public RelationalOp {
private:
    Pipe *g_inPipe, *g_outPipe;
    OrderMaker *g_groupAtts;
    Function *g_computeMe;
    int runLength;
public:
    void Run(Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe);
    void WaitUntilDone();
    void Use_n_Pages(int n);
    void Operation();
};

class WriteOut : public RelationalOp {
private:
    Pipe *wo_inPipe;
    FILE *wo_outFile;
    Schema *wo_mySchema;
    int runLength;
public:
    void Run(Pipe &inPipe, FILE *outFile, Schema &mySchema);
    void WaitUntilDone();
    void Operation();
    void Use_n_Pages(int n);
};
#endif
