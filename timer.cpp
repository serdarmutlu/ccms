#include "include/timer.h"
#include <chrono>
#include "include/main.h"


void Timer::start(int lvl) {
    level = lvl;
    if (level<=TIMER_LEVEL) 
        start_time = std::chrono::high_resolution_clock::now();
}

unsigned long Timer::end() {
    if (level<=TIMER_LEVEL) {
        end_time = std::chrono::high_resolution_clock::now();
        auto delta = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);

    return delta.count();
    } else {
        return 0;
    }
}