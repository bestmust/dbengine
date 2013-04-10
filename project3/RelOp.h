#ifndef REL_OP_H
#define REL_OP_H

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"

class RelationalOp {
	public:
	// blocks the caller until the particular relational operator 
	// has run to completion
	virtual void WaitUntilDone () = 0;

	// tell us how much internal memory the operation can use
	virtual void Use_n_Pages (int n) = 0;
};

class SelectFile : public RelationalOp { 

	private:
        DBFile *sf_inputFile;
        Pipe *sf_outputPipe;
        CNF *sf_selectOperator;
        Record *sf_literalRecord;
        int runLength;
	pthread_t sf_thread;
        
	public:

	void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
        void SF_Operation();
	void WaitUntilDone ();
	void Use_n_Pages (int n);

};

class SelectPipe : public RelationalOp {
    
    
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};
class Project : public RelationalOp { 
private:
    Pipe *p_inPipe,*p_outPipe;
    int *p_keepMe,p_numAttsInput,p_numAttsOutput;
    int runLength;
    pthread_t p_thread;
    
    
	public:
        void P_Operation();
	void Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};
class Join : public RelationalOp { 
	public:
	void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) { }
	void WaitUntilDone () { }
	void Use_n_Pages (int n) { }
};
class DuplicateRemoval : public RelationalOp {
private:
    Pipe *dr_inPipe,*dr_outPipe;
    Schema *dr_mySchema;
    //OrderMaker dr_orderMaker;
    pthread_t dr_thread;
    int runLength;
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
        void DR_Operation();
};
class Sum : public RelationalOp {
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe) { }
	void WaitUntilDone () { }
	void Use_n_Pages (int n) { }
};
class GroupBy : public RelationalOp {
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) { }
	void WaitUntilDone () { }
	void Use_n_Pages (int n) { }
};
class WriteOut : public RelationalOp {
	public:
	void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema) { }
	void WaitUntilDone () { }
	void Use_n_Pages (int n) { }
};
#endif
