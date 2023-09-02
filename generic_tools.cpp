//
// Created by serdar on 6/8/23.
//

#include "include/generic_tools.h"
#include "include/main.h"

#include <iostream>
#include <ratio>
#include <unistd.h>
#include <chrono>

#if defined(__linux__) || defined(__linux) || defined(linux) || defined(__gnu_linux__)
#include <sys/sysinfo.h>
#elif defined(__APPLE__) && defined(__MACH__)
#include <sys/types.h>
#include <sys/sysctl.h>
#endif

void GenericTools::PrintStats(std::string statName) {

    std::cout << statName << std::endl;
#if defined(__linux__) || defined(__linux) || defined(linux) || defined(__gnu_linux__)
    struct sysinfo info;
    sysinfo(&info);
    std::cout << "Memory : " << thousands_separator(info.freeram) << std::endl;
#elif defined(__APPLE__) && defined(__MACH__)
    int mib[2];
    int64_t physical_memory;
    mib[0] = CTL_HW;
    mib[1] = HW_MEMSIZE;
    auto length = sizeof(int64_t);
    sysctl(mib, 2, &physical_memory, &length, NULL, 0);
    std::cout << "Memory : " << thousands_separator(physical_memory) << std::endl;
#endif
    std::cout << "Current Time   :" << thousands_separator(GenericTools::StopWatch::MicroSeconds()) << std::endl << std::endl ;
    std::cout << "Elapsed Time   :" << thousands_separator(GenericTools::StopWatch::MicroSeconds()) << std::endl << std::endl ;
}

void GenericTools::PrintStats() {
    GenericTools::PrintStats("Generic");
};

unsigned long GenericTools::StopWatch::Seconds()
{
    static auto startTime = std::chrono::steady_clock::now();

    auto endTime = std::chrono::steady_clock::now();
    auto delta    = std::chrono::duration_cast<std::chrono::seconds>(endTime - startTime);

    startTime = endTime;

    return delta.count();
}

unsigned long GenericTools::StopWatch::MilliSeconds()
{
    static auto startTime = std::chrono::steady_clock::now();

    auto endTime = std::chrono::steady_clock::now();
    auto delta    = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

    startTime = endTime;

    return delta.count();
}

unsigned long long GenericTools::StopWatch::MicroSeconds()
{
    static auto startTime = std::chrono::steady_clock::now();

    auto endTime = std::chrono::steady_clock::now();
    auto delta    = std::chrono::duration_cast<std::chrono::microseconds>(endTime - startTime);

    startTime = endTime;

    return delta.count();
}

unsigned long long GenericTools::getTotalSystemMemory()
{
    long pages = sysconf(_SC_PHYS_PAGES);
    long pageSize = sysconf(_SC_PAGE_SIZE);
    return pages * pageSize;
}

std::string GenericTools::thousands_separator(long long value, std::string symbol) {
    int length, digitPos, iterator;
    std::string result, valueAsString, u, rev;
    bool min = false;
    result = "";
    digitPos = 0;
    if(value < -999){
        value *= -1;
        min = true;
    }
    valueAsString = std::to_string(value);
    if(value > 999){
        length = valueAsString.length() - 1;
        for (iterator = length; iterator >= 0; iterator--) {
            result += valueAsString[iterator];
            digitPos++;
            if(digitPos%3 == 0){
                result += symbol;
            }
        }
        rev = result;
        result = "";
        length = rev.length() - 1;
        for (iterator = length; iterator >= 0; iterator--) {
            result += rev[iterator];
        }
        u = result[0];
        if(u == symbol){
            result.erase(result.begin());
        }
        if(min){
            result.insert(0, "-");
        }
        return result;
    } else {
        return valueAsString;
    }
}

void GenericTools::Timer::start(int level) {
    this->level = level;
    if (level<=TIMER_LEVEL) 
        startTime = std::chrono::high_resolution_clock::now();
}

unsigned long GenericTools::Timer::end() {
    if (level<=TIMER_LEVEL) {
        endTime = std::chrono::high_resolution_clock::now();
        auto delta = std::chrono::duration_cast<std::chrono::microseconds>(endTime - startTime);

        return delta.count();
    } else {
        return 0;
    }
}