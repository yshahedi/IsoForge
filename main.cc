#include <iostream>
#include <memory>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/prctl.h>
#include <sys/types.h>
#include <fcntl.h>
#include <cstdlib>
#include <signal.h>
#include <fstream>
#include <sstream>
#include "processor.h"

int main(int argc, char *argv[])
{
    std::atomic<uint32_t> counter=0;
    Processor::Processor swch;
    std::shared_ptr<Processor::Config> config=std::make_shared<Processor::Config>();
    config->is_persist_connection=true;
    config->port=5000;
    config->thread_count=5;
    config->worker_count=20;
    config->timeout=500;
    config->processor=[&counter](DL_ISO8583_HANDLER& isoHandler,DL_ISO8583_MSG& isoMsg){
        counter++;
        if(counter.load()%100000==0) std::cout<<counter.load()<<std::endl;
       // DL_ISO8583_MSG_Dump(stdout, NULL, &isoHandler, &isoMsg);
        (void)DL_ISO8583_MSG_SetField_Str(0, (unsigned char *)"1234", &isoMsg);
        (void)DL_ISO8583_MSG_SetField_Str(2, (unsigned char *)"1234567890123456", &isoMsg);
        (void)DL_ISO8583_MSG_SetField_Str(4, (unsigned char *)"5699", &isoMsg);
        (void)DL_ISO8583_MSG_SetField_Str(11, (unsigned char *)"234", &isoMsg);
        (void)DL_ISO8583_MSG_SetField_Str(39, (unsigned char *)"4", &isoMsg);
        (void)DL_ISO8583_MSG_SetField_Str(41, (unsigned char *)"12345", &isoMsg);
        (void)DL_ISO8583_MSG_SetField_Str(42, (unsigned char *)"678901234", &isoMsg);
        (void)DL_ISO8583_MSG_SetField_Str(125, (unsigned char *)"BLAH BLAH", &isoMsg);
    };
    config->timeout_processor=[](DL_ISO8583_HANDLER& isoHandler,DL_ISO8583_MSG& isoMsg){
        
        //DL_ISO8583_MSG_Dump(stdout, NULL, &isoHandler, &isoMsg);
        (void)DL_ISO8583_MSG_SetField_Str(0, (unsigned char *)"1234", &isoMsg);
        (void)DL_ISO8583_MSG_SetField_Str(2, (unsigned char *)"1234567890123456", &isoMsg);
        (void)DL_ISO8583_MSG_SetField_Str(4, (unsigned char *)"5699", &isoMsg);
        (void)DL_ISO8583_MSG_SetField_Str(11, (unsigned char *)"234", &isoMsg);
        (void)DL_ISO8583_MSG_SetField_Str(39, (unsigned char *)"4", &isoMsg);
        (void)DL_ISO8583_MSG_SetField_Str(41, (unsigned char *)"12345", &isoMsg);
        (void)DL_ISO8583_MSG_SetField_Str(42, (unsigned char *)"678901234", &isoMsg);
        (void)DL_ISO8583_MSG_SetField_Str(125, (unsigned char *)"Timeout", &isoMsg);
        DL_ISO8583_MSG_Dump(stdout, NULL, &isoHandler, &isoMsg);
    };
    swch.start(config);
    std::cout << "Done" << std::endl;
    return 0;
}