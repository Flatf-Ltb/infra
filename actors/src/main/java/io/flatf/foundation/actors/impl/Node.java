package io.flatf.foundation.actors.impl;

import java.io.Serial;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Linked Nodes. As a minor efficiency hack, this class opportunistically inherits from AtomicReference, with the atomic ref used as the "next" link.
 * <p>
 * Nodes are in doubly-linked lists. There are three kinds of special nodes, distinguished by: * The list header has a null prev link * The list trailer has a null next link * A deletion marker has a prev link pointing to itself. All three kinds of
 * special nodes have null element fields.
 * <p>
 * Regular nodes have non-null element, next, and prev fields. To avoid visible inconsistencies when deletions overlap element replacement, replacements are done by replacing the node, not just setting the element.
 * <p>
 * Nodes can be traversed by read-only ConcurrentLinkedDeque class operations just by following raw next pointers, so long as they ignore any special nodes seen along the way. (This is automated in method forward.) However, traversal using prev
 * pointers is not guaranteed to see all live nodes since a prev pointer of a deleted node can become unrecoverably stale.
 */
public class Node<E> extends AtomicReference<Node<E>> {

    @Serial
    private static final long serialVersionUID = -1476698285296684641L;

    private volatile Node<E> prev;

    final E element;

    /**
     * Creates a node with given contents
     */
    Node(E element, Node<E> next, Node<E> prev) {
        super(next);
        this.prev = prev;
        this.element = element;
    }

    /**
     * Creates a marker node with given successor
     */
    Node(Node<E> next) {
        super(next);
        this.prev = this;
        this.element = null;
    }

    /**
     * Gets next link (which is actually the value held as atomic reference).
     */
    private Node<E> getNext() {
        return get();
    }

    /**
     * Sets next link
     *
     * @param n the next node
     */
    void setNext(Node<E> n) {
        set(n);
    }

    /**
     * compareAndSet next link
     */
    private boolean casNext(Node<E> cmp, Node<E> val) {
        return compareAndSet(cmp, val);
    }

    /**
     * Gets prev link
     */
    private Node<E> getPrev() {
        return prev;
    }

    /**
     * Sets prev link
     *
     * @param b the previous node
     */
    void setPrev(Node<E> b) {
        prev = b;
    }

    /**
     * Returns true if this is a header, trailer, or marker node
     */
    boolean isSpecial() {
        return element == null;
    }

    /**
     * Returns true if this is a trailer node
     */
    boolean isTrailer() {
        return getNext() == null;
    }

    /**
     * Returns true if this is a header node
     */
    boolean isHeader() {
        return getPrev() == null;
    }

    /**
     * Returns true if this is a marker node
     */
    boolean isMarker() {
        return getPrev() == this;
    }

    /**
     * Returns true if this node is followed by a marker, meaning that it is deleted.
     *
     * @return true if this node is deleted
     */
    boolean isDeleted() {
        Node<E> f = getNext();
        return f != null && f.isMarker();
    }

    /**
     * Returns next node, ignoring deletion marker
     */
    private Node<E> nextNonMarker() {
        Node<E> f = getNext();
        return (f == null || !f.isMarker()) ? f : f.getNext();
    }

    /**
     * Returns the next non-deleted node, swinging next pointer around any encountered deleted nodes, and also patching up successor''s prev link to point back to this. Returns null if this node is trailer so has no successor.
     *
     * @return successor, or null if no such
     */
    Node<E> successor() {
        Node<E> f = nextNonMarker();
        for (; ; ) {
            if (f == null)
                return null;
            if (!f.isDeleted()) {
                if (f.getPrev() != this && !isDeleted())
                    f.setPrev(this); // relink f's prev
                return f;
            }
            Node<E> s = f.nextNonMarker();
            if (f == getNext())
                casNext(f, s); // unlink f
            f = s;
        }
    }

    /**
     * Returns the apparent predecessor of target by searching forward for it starting at this node, patching up pointers while traversing. Used by predecessor().
     *
     * @return target's predecessor, or null if not found
     */
    private Node<E> findPredecessorOf(Node<E> target) {
        Node<E> n = this;
        for (; ; ) {
            Node<E> f = n.successor();
            if (f == target)
                return n;
            if (f == null)
                return null;
            n = f;
        }
    }

    /**
     * Returns the previous non-deleted node, patching up pointers as needed. Returns null if this node is header so has no successor. May also return null if this node is deleted, so doesn't have a distinct predecessor.
     *
     * @return predecessor or null if not found
     */
    Node<E> predecessor() {
        Node<E> n = this;
        for (; ; ) {
            Node<E> b = n.getPrev();
            if (b == null)
                return n.findPredecessorOf(this);
            Node<E> s = b.getNext();
            if (s == this)
                return b;
            if (s == null || !s.isMarker()) {
                Node<E> p = b.findPredecessorOf(this);
                if (p != null)
                    return p;
            }
            n = b;
        }
    }

    /**
     * Returns the next node containing a nondeleted user element. Use for forward list traversal.
     *
     * @return successor, or null if no such
     */
    Node<E> forward() {
        Node<E> f = successor();
        return (f == null || f.isSpecial()) ? null : f;
    }

    /**
     * Returns previous node containing a nondeleted user element, if possible. Use for backward list traversal, but beware that if this method is called from a deleted node, it might not be able to determine a usable predecessor.
     *
     * @return predecessor, or null if no such could be found
     */
    Node<E> back() {
        Node<E> f = predecessor();
        return (f == null || f.isSpecial()) ? null : f;
    }

    /**
     * Tries to insert a node holding element as successor, failing if this node is deleted.
     *
     * @param element the element
     * @return the new node, or null on failure.
     */
    Node<E> append(E element) {
        for (; ; ) {
            Node<E> f = getNext();
            if (f == null || f.isMarker())
                return null;
            Node<E> x = new Node<>(element, f, this);
            if (casNext(f, x)) {
                f.setPrev(x); // optimistically link
                return x;
            }
        }
    }

    /**
     * Tries to insert a node holding element as predecessor, failing if no live predecessor can be found to link to.
     *
     * @param element the element
     * @return the new node, or null on failure.
     */
    Node<E> prepend(E element) {
        for (; ; ) {
            Node<E> b = predecessor();
            if (b == null)
                return null;
            Node<E> x = new Node<>(element, this, b);
            if (b.casNext(this, x)) {
                setPrev(x); // optimistically link
                return x;
            }
        }
    }


    /**
     * Tries to mark this node as deleted, failing if already deleted or if this node is header or trailer
     *
     * @return true if successful
     */
    boolean delete() {
        Node<E> b = getPrev();
        Node<E> f = getNext();
        if (b != null && f != null && !f.isMarker()
                && casNext(f, new Node<>(f))) {
            if (b.casNext(this, f))
                f.setPrev(b);
            return true;
        }
        return false;
    }

    /**
     * Tries to insert a node holding element to replace this node. failing if already deleted.
     *
     * @param newElement the new element
     * @return the new node, or null on failure.
     */
    Node<E> replace(E newElement) {
        for (; ; ) {
            Node<E> b = getPrev();
            Node<E> f = getNext();
            if (b == null || f == null || f.isMarker())
                return null;
            Node<E> x = new Node<>(newElement, f, b);
            if (casNext(f, new Node<>(x))) {
                b.successor(); // to relink b
                x.successor(); // to relink f
                return x;
            }
        }
    }
}

