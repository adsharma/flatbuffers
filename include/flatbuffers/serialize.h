#pragma once

#include <endian.h>
#include <stdint.h>
#include <string>

namespace keyencoder {

// Serialize type T into a byte comparable
// string in-place
template<typename T>
void Serialize(T *val);

template<typename T>
void Deserialize(T *val);

// Specializations
template<>
inline void Serialize(int64_t *val) {
  *val ^= (1UL << 63);
  *val = htobe64(*val);
}

template<>
inline void Serialize(double *val) {
  int64_t mask = -1;
  if (*val >= 0) {
    mask = 1UL << 63;
  }
  int64_t buf;
  int64_t *ivalp = (int64_t *) val;
  buf = *ivalp;
  buf ^= mask;
  buf = (int64_t) htobe64(buf);
  *ivalp = buf;
}

template<typename T>
inline void Serialize(T *val) {
  return;
}

template<>
inline void Deserialize(int64_t *val) {
  auto tmp = *val;
  tmp = be64toh(tmp);
  tmp ^= (1UL << 63);
  *val = tmp;
}

template<>
inline void Deserialize(double *val) {
  auto tmp = *(int64_t *) val;
  int64_t mask = -1;
  tmp = be64toh(tmp);
  auto *tmpd = (double *) &tmp;
  if (*tmpd < 0) {
    mask = 1UL << 63;
  }
  tmp ^= mask;
  *val = *tmpd;
}

template<typename T>
inline void Deserialize(T *val) {
  return;
}

}
