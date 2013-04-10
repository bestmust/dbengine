#ifndef DEFS_H
#define DEFS_H
#define MANUALTEST 0
#define MANUALDEBUG 1
#if MANUALDEBUG
#define MDLog(x,y)  ( cout<<"\n"<<x<<" - "<<y )
#else
#define MDLog(x,y) ( cout<<"" )
#endif

#define MAX_ANDS 20
#define MAX_ORS 20
#define PIPE_BUFF_SIZE 200
#define PAGE_SIZE 131072


enum Target {Left, Right, Literal};
enum CompOperator {LessThan, GreaterThan, Equals};
enum Type {Int, Double, String};


unsigned int Random_Generate();






#endif

