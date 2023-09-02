#ifndef TIMER_H
#define TIMER_H

#include "ccms.h"

#include <chrono>
#include <ratio>

class Timer {
private:
    int level;
    std::chrono::high_resolution_clock::time_point start_time, end_time;
public:
    void start(int lvl);
    unsigned long end();
    int getLevel() const { return level; }
    void setLevel(int level_) { level = level_; }
};

#endif //TIMER_H