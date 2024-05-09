package btree;

import java.io.Serializable;
import java.util.*;
import java.util.ArrayList;
import java.util.Vector;
import db.Tuples;

/**
 * A B+ tree Since the structures and behaviors between internal node and
 * external node are different, so there are two different classes for each kind
 * of node.
 *
 * @param < TKey > the data type of the key
 * @param < TValue > the data type of the value
 */
public class BTree<TKey extends Comparable<TKey>, TValue> implements Serializable {
  
    private BTreeNode<TKey> root;
    private String tableName;
    private Vector <TKey> keys = new Vector<TKey>();
    private Vector <TKey> duplicateKeys = new Vector<TKey>();
    private Vector <String> duplicateKeysPages = new Vector<String>();

    public BTree() {
        this.root = new BTreeLeafNode<TKey, TValue>();
    }
    
	public Vector <TKey> getKeys() {
		return keys;
	}

	public void addKeys(TKey key) {
		
		keys.add(key);
		keys.sort(null);
	}
	
	public void removeKeys(TKey key) {
		keys.remove(key);
	}
	
	public void updateTree(BTree btree, TKey oldKey, TKey newKey, String pageName) {
		btree.delete(oldKey);
		btree.insert(newKey, pageName);
	}
	
	
	public void removeDuplicatePages(int i) {
		duplicateKeysPages.remove(i);
	}
	
	public void removeDuplicateKey (int i) {
		duplicateKeys.remove(i);
	}
	
	
	
    /**
     * Insert a new key and its associated value into the B+ tree.
     */
    public void insert(TKey key, TValue value) {
        BTreeLeafNode<TKey, TValue> leaf = this.findLeafNodeShouldContainKey(key);
        boolean flag = false;
        for (int i =0; i<keys.size();i++) {
        	if (key.compareTo(keys.get(i))==0) {
        		duplicateKeys.add( keys.get(i));
        		duplicateKeysPages.add((String) search(keys.get(i)));
        		leaf.insertKey(key, value);
        		duplicateKeys.add( key);
        		duplicateKeysPages.add((String) search(key));
        		flag = true;
        		break;
        	}
        }
        if (flag == false)
        	leaf.insertKey(key, value);

        if (leaf.isOverflow()) {
            BTreeNode<TKey> n = leaf.dealOverflow();
            if (n != null)
                this.root = n;
        }
    }

    public Vector getDuplicateKeys() {
		return duplicateKeys;
	}
    public Vector getDuplicateKeysPages() {
		return duplicateKeysPages;
	}

	/**
     * Search a key value on the tree and return its associated value.
     */
    public TValue search(TKey key) {
        BTreeLeafNode<TKey, TValue> leaf = this.findLeafNodeShouldContainKey(key);

        int index = leaf.search(key);
        return (index == -1) ? null : leaf.getValue(index);
    }

    /**
     * Delete a key and its associated value from the tree.
     */
    public void delete(TKey key) {
        BTreeLeafNode<TKey, TValue> leaf = this.findLeafNodeShouldContainKey(key);

        if (leaf.delete(key) && leaf.isUnderflow()) {
            BTreeNode<TKey> n = leaf.dealUnderflow();
            if (n != null)
                this.root = n;
        }
    }

    /**
     * Search the leaf node which should contain the specified key
     */
    @SuppressWarnings("unchecked")
    private BTreeLeafNode<TKey, TValue> findLeafNodeShouldContainKey(TKey key) {
        BTreeNode<TKey> node = this.root;
        while (node.getNodeType() == TreeNodeType.InnerNode) {
            node = ((BTreeInnerNode<TKey>) node).getChild(node.search(key));
        }

        return (BTreeLeafNode<TKey, TValue>) node;
    }

    public void print() {
        ArrayList<BTreeNode> upper = new ArrayList<>();
        ArrayList<BTreeNode> lower = new ArrayList<>();

        upper.add(root);
        while (!upper.isEmpty()) {
            BTreeNode cur = upper.get(0);
            if (cur instanceof BTreeInnerNode) {
                ArrayList<BTreeNode> children = ((BTreeInnerNode) cur).getChildren();
                for (int i = 0; i < children.size(); i++) {
                    BTreeNode child = children.get(i);
                    if (child != null)
                        lower.add(child);
                }
            }
            System.out.println(cur.toString() + " ");
            upper.remove(0);
            if (upper.isEmpty()) {
                System.out.println("\n");
                upper = lower;
                lower = new ArrayList<>();
            }
        }
    }

    public BTreeLeafNode getSmallest() {
        return this.root.getSmallest();
    }

    public String commit() {
        return this.root.commit();
    }




}