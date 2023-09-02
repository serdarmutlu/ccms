
#include <string>
#include <iostream>

int main() {
    const char *keys[20];
    const char *values[20];
    int index{0};
    std::string hostName = "Hello";
    keys[index] = "host";
    values[index++] = hostName.c_str();

    std::cout << "idx:" << index << " host1:" << hostName << " host2:" << values[index-1] << std::endl;
}