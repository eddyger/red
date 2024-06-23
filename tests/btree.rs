use red::database::abstraction::{BPlusTree, NodeType};

#[test]
fn test_btree_creation() {
    let mut btree = BPlusTree::new(3);
    assert_eq!(btree.get_order(), 3);
    assert!(btree.get_root_node().is_none());
    assert_eq!(0, btree.get_tree_height());
}

#[test]
fn test_btree_add_leaf() {
    let mut btree = BPlusTree::new(2);
    assert_eq!(btree.get_order(), 2);
    assert!(btree.get_root_node().is_none());
    assert_eq!(0, btree.get_tree_height());

    btree.insert(7);
    assert_eq!(1, btree.get_tree_height());
    assert_eq!("LeafNode", type_of_node(btree.get_root_node().unwrap()));

    btree.insert(10);
    assert_eq!(1, btree.get_tree_height());
    assert_eq!("LeafNode", type_of_node(btree.get_root_node().unwrap()));

    btree.insert(15);
    assert_eq!(2, btree.get_tree_height());
    assert_eq!("InternalNode", type_of_node(btree.get_root_node().unwrap()));
}

fn type_of_node(node: &NodeType) -> String {
    match node {
        NodeType::Leaf(_) => "LeafNode".to_string(),
        NodeType::Internal(_) => "InternalNode".to_string(),
    }
}
