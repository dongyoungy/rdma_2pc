#include <random>
#include <cmath>
#include <ctime>
#include <algorithm>
#include <iostream>

using namespace std;

long getZipfRand(double p, double s, double N) {
  double tolerance = 0.01;
  double x = N/2;
  double D = p * (12 * (pow(N, 1-s) -1 ) / (1-s) + 6 - 6 * pow(N, -1 * s) + s - pow(N, -1 - s) * s);
  while (true) {
    double m = pow(x, -2 - s);
    double mx = m*x;
    double mxx = mx * x;
    double mxxx = mxx * x;

    double a = 12 * (mxxx-1) / (1-s) + 6 * (1-mxx) + (s -(mx*s)) -D;
    double b = 12 * mxx + 6 * (s * mx) + (m * s * (s+1));
    double newx = max(1.0, x - a/b);
    if (abs(newx-x) <= tolerance)
      return (long) newx;
    x = newx;
  }
}

int main() {
  srand(time(NULL));
  cout << "Zipf:" << endl;
  for (int i = 0; i < 10; ++i) {
    cout << getZipfRand(((double)rand()/(RAND_MAX)), 0.99, 100) - 1 << endl;
  }
  default_random_engine gen(time(NULL));
  uniform_int_distribution<int> dist(0, 100);
  cout << endl << "Uniform:" << endl;
  for (int i = 0; i < 10; ++i) {
    cout << dist(gen) << endl;
  }

  return 0;
}

