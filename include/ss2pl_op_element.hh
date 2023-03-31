#pragma once

#include <memory>

#include "../../include/op_element.hh"
#include "../../include/tuple_body.hh"

template<typename T>
class ReadElement : public OpElement<T> {
public:
  using OpElement<T>::OpElement;
  TupleBody body_;

  ReadElement(Storage s, std::string_view key, T* rcdptr, TupleBody&& body)
          : OpElement<T>::OpElement(s, key, rcdptr), body_(std::move(body)) {}

  bool operator<(const ReadElement &right) const {
    if (this->storage_ != right.storage_) return this->storage_ < right.storage_;
    return this->key_ < right.key_;
  }
};

template<typename T>
class WriteElement : public OpElement<T> {
public:
  using OpElement<T>::OpElement;
  TupleBody body_;

  WriteElement(Storage s, std::string_view key, T* rcdptr, TupleBody&& body, OpType op)
          : OpElement<T>::OpElement(s, key, rcdptr, op), body_(std::move(body)) {
  }

  bool operator<(const WriteElement &right) const {
    if (this->storage_ != right.storage_) return this->storage_ < right.storage_;
    return this->key_ < right.key_;
  }
};
