#include "primer/trie.h"
#include <sys/types.h>
#include <iostream>
#include <memory>
#include <string_view>
#include <utility>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.

  if (key.empty()) {
    auto val = dynamic_cast<const TrieNodeWithValue<T> *>(root_.get());
    if (val == nullptr) {
      return nullptr;
    }
    return val->value_.get();
  }

  if (root_ == nullptr) {
    return nullptr;
  }
  std::shared_ptr<const bustub::TrieNode> cur = this->root_;

  for (auto ch : key) {
    if (cur->children_.count(ch)) {
      cur = cur->children_.at(ch);
    } else {
      return nullptr;
    }
  }

  auto val = dynamic_cast<const TrieNodeWithValue<T> *>(cur.get());
  if (val == nullptr) {
    return nullptr;
  }
  return val->value_.get();
}

template <class T>
auto PutNode(const std::shared_ptr<bustub::TrieNode> &root, std::string_view key, T value) -> void {
  char ch = key[0];
  if (root->children_.count(ch)) {
    if (key.size() == 1) {
      auto val_n = std::make_shared<T>(std::move(value));
      auto new_node = TrieNodeWithValue<T>(root->children_.at(ch)->children_, val_n);
      root->children_.at(ch) = new_node.Clone();
    } else {
      std::shared_ptr<TrieNode> p = root->children_.at(ch)->Clone();
      PutNode(p, key.substr(1), std::move(value));
      root->children_.at(ch) = std::shared_ptr<const TrieNode>(p);
    }
  } else {
    if (key.size() == 1) {
      auto val_n = std::make_shared<T>(std::move(value));
      root->children_.insert({ch, std::make_shared<const TrieNodeWithValue<T>>(val_n)});
    } else {
      auto p = std::make_shared<TrieNode>();
      PutNode(p, key.substr(1), std::move(value));
      root->children_.insert({ch, std::move(p)});
    }
  }
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // throw NotImplementedException("Trie::Put is not implemented.");
  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.

  if (key.empty()) {
    std::shared_ptr<TrieNodeWithValue<T>> new_node =
        std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
    for (auto [c, v] : root_->children_) {
      new_node->children_.insert({c, v});
    }
    return Trie(std::move(new_node));
  }
  std::shared_ptr<TrieNode> p = nullptr;
  if (this->root_ == nullptr) {
    p = std::make_shared<TrieNode>();
  } else {
    p = root_->Clone();
  }
  auto new_root = p;
  PutNode<T>(p, key, std::move(value));
  return Trie(std::move(new_root));
}

auto RemoveNode(std::string_view key, std::shared_ptr<TrieNode> &root) {
  char ch = key[0];
  if (root->children_.count(ch) == 0) {
    return false;
  }
  auto &son = root->children_.at(ch);
  if (key.size() == 1) {
    if (!son->is_value_node_) {
      return false;
    }
    if (son->children_.empty()) {
      root->children_.erase(ch);
    } else {
      son = std::make_shared<const TrieNode>(son->children_);
    }
  } else {
    std::shared_ptr<TrieNode> ptr = son->Clone();
    if (!RemoveNode(key.substr(1), ptr)) {
      root->children_.at(ch) = ptr;
      return false;
    }
    if (!ptr->is_value_node_ && ptr->children_.empty()) {
      root->children_.erase(ch);
    } else {
      root->children_.at(ch) = ptr;
    }
  }
  return true;
}
auto Trie::Remove(std::string_view key) const -> Trie {
  // throw NotImplementedException("Trie::Remove is not implemented.");
  //  You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  //  you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
  if (root_ == nullptr) {
    return Trie(nullptr);
  }
  std::shared_ptr<TrieNode> p = root_->Clone();
  if (!RemoveNode(key, p)) {
    return Trie(nullptr);
  }
  if (p->children_.empty() && !p->is_value_node_) {
    return Trie(nullptr);
  }
  return Trie(p);
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
