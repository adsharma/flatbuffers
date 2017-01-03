#pragma once

#include "flatbuffers/flatbuffers.h"

namespace flatbuffers {

// Like FlatBufferBuilder, but optimized
// for a key-value store.
// Usage:
//
// KVStoreBuilder kvb;
// ConcreteVBuilder ckvb(kvb);
//
// ckvb.add_k1(key1);
// ckvb.add_k2(key2);
// ckvb.add_k3(key3);
//
// ckvb.add_v1(val1);
// ckvb.add_v2(val2);
// ckvb.add_v3(val3);
//
// It maintains two buffers internally - one
// for key, one for value. In the example
// above, k1-k3 go to the first buffer and v1-v3
// go to the second buffer.
//
// However, key strings and key vectors are an exception
// The actual data goes into the key and the
// metadata (length and relative offset) goes into value.
//
// The order in which add_*() is called needs to match
// the order which they are defined in the schema.
class KVStoreBuilder {
 public:
  explicit KVStoreBuilder(uoffset_t initial_size = 1024,
                          const simple_allocator *allocator = nullptr)
  : key_builder_(initial_size, allocator)
  , value_builder_(initial_size, allocator) {
    key_builder_.setAlign(false);
  }

  void Clear() {
    key_builder_.Clear();
    value_builder_.Clear();
  }

  uoffset_t GetSize() const {
    return key_builder_.GetSize();
  }

  const uint8_t *GetKey() const { return key_pointer_; }
  const uint8_t *GetValue() const { return value_pointer_; }

  uoffset_t GetKeySize() const { return key_size_; }
  uoffset_t GetValueSize() const { return value_size_; }

  template<typename T> void AddElement(voffset_t field, T e, T def) {
    // For backward compatibility. Generated code calls Add{Key,Value}Element
    // variants.
    key_builder_.AddElement<T>(field, e, def);
  }

  template<typename T> void AddKeyElement(voffset_t field, T e, T def) {
    key_builder_.AddElement<T>(field, e, def);
  }

  template<typename T> void AddValueElement(voffset_t field, T e, T def) {
    value_builder_.AddElement<T>(field, e, def);
  }

  // offsets to value_builder, where they don't interfere with sorting
  template<typename T> void AddOffset(voffset_t field, Offset<T> off) {
    value_builder_.AddOffset<T>(field, off);
  }

  template<typename T> void AddKeyOffset(voffset_t field, Offset<T> off) {
    // Need to convert key offset to value offset to have a valid
    // offset in the vtable. Not clear if ReferTo() is doing the right thing.
    off = 0;
    value_builder_.AddOffset<T>(field, off);
  }

  Offset<String> CreateString(const char *str, size_t len) {
    return key_builder_.CreateString(str, len);
  }

  /// @brief Store a string in the buffer, which is null-terminated.
  /// @param[in] str A const char pointer to a C-string to add to the buffer.
  /// @return Returns the offset in the buffer where the string starts.
  Offset<String> CreateString(const char *str) {
    return CreateString(str, strlen(str));
  }

  /// @brief Store a string in the buffer, which can contain any binary data.
  /// @param[in] str A const reference to a std::string to store in the buffer.
  /// @return Returns the offset in the buffer where the string starts.
  Offset<String> CreateString(const std::string &str) {
    return CreateString(str.c_str(), str.length());
  }

  /// @brief Store a string in the buffer, which can contain any binary data.
  /// @param[in] str A const pointer to a `String` struct to add to the buffer.
  /// @return Returns the offset in the buffer where the string starts
  Offset<String> CreateString(const String *str) {
    return str ? CreateString(str->c_str(), str->Length()) : 0;
  }

  Offset<String> CreateValueString(const char *str, size_t len) {
    return value_builder_.CreateString(str, len);
  }

  /// @brief Store a string in the buffer, which is null-terminated.
  /// @param[in] str A const char pointer to a C-string to add to the buffer.
  /// @return Returns the offset in the buffer where the string starts.
  Offset<String> CreateValueString(const char *str) {
    return CreateValueString(str, strlen(str));
  }

  /// @brief Store a string in the buffer, which can contain any binary data.
  /// @param[in] str A const reference to a std::string to store in the buffer.
  /// @return Returns the offset in the buffer where the string starts.
  Offset<String> CreateValueString(const std::string &str) {
    return CreateValueString(str.c_str(), str.length());
  }

  /// @brief Store a string in the buffer, which can contain any binary data.
  /// @param[in] str A const pointer to a `String` struct to add to the buffer.
  /// @return Returns the offset in the buffer where the string starts
  Offset<String> CreateValueString(const String *str) {
    return str ? CreateValueString(str->c_str(), str->Length()) : 0;
  }

  void Finished() const {
    key_builder_.Finished();
  }

  template<typename T> void Finish(Offset<T> root,
                                   const char *file_identifier = nullptr) {
    key_builder_.Finish<T>(root, file_identifier);
  }

  template<typename T> void Required(Offset<T> table, voffset_t field) {
    key_builder_.Required<T>(table, field);
  }

  uint8_t *GetBufferPointer() const {
    return key_builder_.GetBufferPointer();
  }

  unique_ptr_t ReleaseBufferPointer() {
    key_builder_.Finished();
    value_builder_.Finished();
    // TODO: we need to make sure that the two builders
    // have their memory "almost" contiguously, so
    // we can return a single GetSize()
    value_builder_.ReleaseBufferPointer();
    return key_builder_.ReleaseBufferPointer();
  }

  uoffset_t StartTable() {
    key_builder_.StartTable();
    key_start_ = key_builder_.GetSize();
    value_builder_.StartTable();
    value_start_ = value_builder_.GetSize();
    return key_start_;
  }

  uoffset_t EndTable(uoffset_t /* start */, voffset_t numfields) {
    int num_value_fields = 3; // TODO: generate it
    int num_key_fields = 3; // TODO: generate it
    int num_fixed_fields = 2; // TODO: should be a constant somewhere

    key_pointer_ = key_builder_.GetCurrentBufferPointer();
    key_size_ = key_builder_.GetSize();

    value_pointer_ = value_builder_.GetCurrentBufferPointer();
    value_size_ = value_builder_.GetSize();

    value_builder_.EndTable(value_start_, num_value_fields);

    uint8_t *valuep = value_builder_.GetCurrentBufferPointer();

    // This is similar to GetAnyRoot(), but we can't use the GetAnyRoot()
    // here, because Finish<>() hasn't been called yet.
    voffset_t vtable_size = *reinterpret_cast<voffset_t *>(valuep)
      + sizeof(soffset_t);
    const uint8_t *tablep = valuep + vtable_size;
    const Table *table = reinterpret_cast<const Table *>(tablep) - sizeof(soffset_t);
    soffset_t pushed_bytes = value_builder_.GetSize() - vtable_size;

    key_builder_.PushBytes(tablep, pushed_bytes);
    voffset_t kfield = (num_key_fields + num_fixed_fields) * 2;
    voffset_t vfield = num_fixed_fields * 2;

    // Relocate the offsets in the value_builder_ vtable
    for (int i = 0; i < num_value_fields; i++) {
      // off relative to the table growing upwards
      uoffset_t off = table->GetOptionalFieldOffset(vfield);
      // soffset_t for the vtable_size that hasn't been written yet
      uoffset_t off_from_end = key_builder_.GetSize() + sizeof(soffset_t) - off;
      key_builder_.TrackField(kfield, off_from_end);
      kfield += 2;
      vfield += 2;
    }
    // Relocate the offsets in relocs_. Primarily indirect strings
    // and vectors
    for (const auto& r : relocs_) {
      // r is relative to the end of the value_builder_.buf_
      // convert it to a pointer relative to the end of
      // key_builder_.buf_
      auto offp = key_builder_.GetCurrentBufferPointer() + pushed_bytes - r;
      auto p = reinterpret_cast<uoffset_t *>(offp);
      auto uint8p = reinterpret_cast<uint8_t *>(p);
      // Convert offsets from being relative to end of buffer to beginning of
      // table and then relative to p
      auto adj = uint8p - key_builder_.GetCurrentBufferPointer() + sizeof(soffset_t);
      *p = key_builder_.ReferTo(*p) - adj;
    }
    auto ret = key_builder_.EndTable(key_start_, numfields);
    return ret;
  }

 private:
  uoffset_t key_start_;
  uoffset_t value_start_;
  uoffset_t key_size_;
  uoffset_t value_size_;
  uint8_t *key_pointer_;
  uint8_t *value_pointer_;
  FlatBufferBuilder key_builder_;
  FlatBufferBuilder value_builder_;
  // After pushing value buffer, the offsets in the vtable
  // are adjusted to account that. relocs_ provides the
  // same functionality for references not in vtable that
  // need to be adjusted
  std::vector<uoffset_t> relocs_;
};

template<> void
KVStoreBuilder::AddKeyElement(voffset_t field, const std::string e,
                              const std::string /* def */) {
  uoffset_t len = static_cast<uoffset_t>(e.size());
  key_builder_.PushBytes(reinterpret_cast<const uint8_t *>(e.c_str()), len + 1);
  uoffset_t off = key_builder_.GetSize();
  value_builder_.PushElement(off);
  relocs_.push_back(value_builder_.GetSize());
  value_builder_.AddElement(field, len, static_cast<uoffset_t>(0));
}

}  // namespace flatbuffers
