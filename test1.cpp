
#include <iostream>
#include <string>
#include <string_view>
#include <vector>

struct Student {
  int id;
  std::string first_name;
  std::string last_name;
  // Student(int id, std::string first_name, std::string last_name) {
  //     this->id = id;
  //     this->first_name = first_name;
  //     this->last_name = last_name;
  // }
};

void xx(std::string y) { std::cout << y << std::endl; }

int main()

{
  std::vector<Student> v;

  for (int i = 0; i < 10; i++) {
    // Student* student = new Student(i, "Serdar", "Mutlu");
    Student student;
    student.id = i;
    student.first_name = "Serdar";
    student.last_name = "Mutlu";
    v.push_back(student);

    // delete student;
  }

  for (unsigned long i = 0; i < v.size(); i++) {
    Student x = v.at(i);
    std::cout << x.id << " " << x.first_name << " " << x.last_name << " "
              << std::endl;
  }
}
