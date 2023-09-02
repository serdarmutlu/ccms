//
// Created by serdar on 6/8/23.
//

#ifndef GENERIC_TOOLS_H
#define GENERIC_TOOLS_H

#include "ccms.h"

#include <string>

#if defined(__linux__) || defined(__linux) || defined(linux) || defined(__gnu_linux__)
#include <sys/sysinfo.h>
#elif defined(__APPLE__) && defined(__MACH__)
#include <sys/types.h>
#include <sys/sysctl.h>
#endif

#include <chrono>

class GenericTools {
    class Timer {
    private:
        int level;
        std::chrono::high_resolution_clock::time_point startTime, endTime;
        
    public:
        void start(int lvl);
        unsigned long end();
    };
public:
    static unsigned long long getTotalSystemMemory();
    static std::string thousands_separator(long long k, std::string symbol=",");
    static void PrintStats(std::string statName);
    static void PrintStats();
    class StopWatch {
        public:
            static unsigned long Seconds();
            static unsigned long MilliSeconds();
            static unsigned long long MicroSeconds();
    };

};

#endif //GENERIC_TOOLS_H