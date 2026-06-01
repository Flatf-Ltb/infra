package io.flatf.foundation.actors.impl;

/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the private domain, as explained at
 * http://creativecommons.org/licenses/privatedomain
 */

import javax.annotation.Nonnull;
import java.io.Serial;
import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Deque;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.IntFunction;

/**
 * A concurrent linked-list implementation of a {@link Deque} (double-ended queue). Concurrent insertion, removal, and access operations execute safely across multiple threads. Iterators are <i>weakly consistent</i>, returning elements reflecting the
 * state of the deque at some point at or since the creation of the iterator. They do <em>not</em> throw {@link ConcurrentModificationException}, and may proceed concurrently with other operations.
 *
 * <p>
 * This class and its iterators implement all of the <em>optional</em> methods of the {@link Collection} and {@link Iterator} interfaces. Like most other concurrent collection implementations, this class does not permit the use of <tt>null</tt>
 * elements. because some null arguments and return values cannot be reliably distinguished from the absence of elements. Arbitrarily, the {@link Collection#remove} method is mapped to <tt>removeFirstOccurrence</tt>, and {@link Collection#add} is
 * mapped to <tt>addLast</tt>.
 *
 * <p>
 * Beware that, unlike in most collections, the <tt>size</tt> method is <em>NOT</em> a constant-time operation. Because of the asynchronous nature of these dequeue, determining the current number of elements requires a traversal of the elements.
 *
 * <p>
 * This class is <tt>Serializable</tt>, but relies on default serialization mechanisms. Usually, it is a better idea for any serializable class using a <tt>ConcurrentLinkedDeque</tt> to instead serialize a snapshot of the elements obtained by method
 * <tt>toArray</tt>.
 *
 * @param <E> the type of elements held in this collection
 * @author Doug Lea
 */

public class ConcurrentDoublyLinkedList<E> extends AbstractCollection<E>
        implements java.io.Serializable {

    @Override
    public <T> T[] toArray(IntFunction<T[]> generator) {
        return super.toArray(generator);
    }

    /*
     * This is an adaptation of an algorithm described in Paul Martin's "A Practical Lock-Free Doubly-Linked List". Sun Labs Tech report. The basic idea is to primarily rely on next-pointers to ensure consistency. Prev-pointers are in part
     * optimistic, reconstructed using forward pointers as needed. The main forward list uses a variant of HM-list algorithm similar to the one used in ConcurrentSkipListMap class, but a little simpler. It is also basically similar to the approach in
     * Edya Ladan-Mozes and Nir Shavit "An Optimistic Approach to Lock-Free FIFO Queues" in DISC04.
     *
     * Quoting a summary in Paul Martin's tech report:
     *
     * All cleanups work to maintain these invariants: (1) forward pointers are the ground truth. (2) forward pointers to dead nodes can be improved by swinging them further forward around the dead node. (2.1) forward pointers are still correct when
     * pointing to dead nodes, and forward pointers from dead nodes are left as they were when the node was deleted. (2.2) multiple dead nodes may point forward to the same node. (3) backward pointers were correct when they were installed (3.1)
     * backward pointers are correct when pointing to any node which points forward to them, but since more than one forward pointer may point to them, the live one is best. (4) backward pointers that are out of date due to deletion point to a
     * deleted node, and need to point further back until they point to the live node that points to their source. (5) backward pointers that are out of date due to insertion point too far backwards, so shortening their scope (by searching forward)
     * fixes them. (6) backward pointers from a dead node cannot be "improved" since there may be no live node pointing forward to their origin. (However, it does no harm to try to improve them while racing with a deletion.)
     *
     *
     * Notation guide for local variables n, b, f : a node, its predecessor, and successor s : some other successor
     */

    // Minor convenience utilities

    /**
     * Returns true if given reference is non-null and isn't a header, trailer, or marker.
     *
     * @param node (possibly null) node
     * @return true if n exists as a user node
     */
    private static boolean usable(Node<?> node) {
        return node != null && !node.isSpecial();
    }

    /**
     * Throws NullPointerException if argument is null
     *
     * @param obj the element
     */
    private static void checkNullArg(Object obj) {
        if (obj == null)
            throw new NullPointerException();
    }

    /**
     * Creates an array list and fills it with elements of this list. Used by toArray.
     *
     * @return the arrayList
     */
    private ArrayList<E> toArrayList() {
        ArrayList<E> c = new ArrayList<>();
        for (Node<E> n = header.forward(); n != null; n = n.forward())
            c.add(n.element);
        return c;
    }

    // Fields and constructors

    @Serial
    private static final long serialVersionUID = 876323262645176354L;

    /**
     * List header. First usable node is at header.forward().
     */
    private final Node<E> header;

    /**
     * List trailer. Last usable node is at trailer.back().
     */
    private final Node<E> trailer;

    /**
     * Constructs an empty deque.
     */
    ConcurrentDoublyLinkedList() {
        Node<E> header0 = new Node<>(null, null, null);
        Node<E> trailer0 = new Node<>(null, null, header0);
        header0.setNext(trailer0);
        this.header = header0;
        this.trailer = trailer0;
    }

    /**
     * Appends the given element to the end of this deque. This is identical in function to the <tt>add</tt> method.
     *
     * @param o the element to be inserted at the end of this deque.
     * @throws NullPointerException if the specified element is <tt>null</tt>
     */
    private void addLast(E o) {
        checkNullArg(o);
        while (trailer.prepend(o) == null)
            ;
    }

    Node<E> coolAdd(E element) {
        Node<E> node;
        while ((node = trailer.prepend(element)) == null)
            ;
        return node;
    }

    /**
     * Appends the given element to the end of this deque. (Identical in function to the <tt>add</tt> method; included only for consistency.)
     *
     * @param o the element to be inserted at the end of this deque.
     * @return <tt>true</tt> always
     * @throws NullPointerException if the specified element is <tt>null</tt>
     */
    private boolean offerLast(E o) {
        addLast(o);
        return true;
    }

    /**
     * Retrieves and removes the first element of this deque, or returns null if this deque is empty.
     *
     * @return the first element of this deque, or <tt>null</tt> if empty.
     */
    private E pollFirst() {
        for (; ; ) {
            Node<E> n = header.successor();
            if (!usable(n))
                return null;
            if (n.delete())
                return n.element;
        }
    }

    @Override
    public boolean add(E e) {
        return offerLast(e);
    }

    /**
     * Removes the first element <tt>e</tt> such that <tt>o.equals(e)</tt>, if such an element exists in this deque. If the deque does not contain the element, it is unchanged.
     *
     * @param o element to be removed from this deque, if present.
     * @return <tt>true</tt> if the deque contained the specified element.
     * @throws NullPointerException if the specified element is <tt>null</tt>
     */
    private boolean removeFirstOccurrence(Object o) {
        checkNullArg(o);
        for (; ; ) {
            Node<E> n = header.forward();
            for (; ; ) {
                if (n == null)
                    return false;
                if (o.equals(n.element)) {
                    if (n.delete())
                        return true;
                    else
                        break; // restart if interference
                }
                n = n.forward();
            }
        }
    }

    /**
     * Returns <tt>true</tt> if this deque contains at least one element <tt>e</tt> such that <tt>o.equals(e)</tt>.
     *
     * @param o element whose presence in this deque is to be tested.
     * @return <tt>true</tt> if this deque contains the specified element.
     */
    @Override
    public boolean contains(Object o) {
        if (o == null)
            return false;
        for (Node<E> n = header.forward(); n != null; n = n.forward())
            if (o.equals(n.element))
                return true;
        return false;
    }

    /**
     * Returns <tt>true</tt> if this collection contains no elements.
     * <p>
     *
     * @return <tt>true</tt> if this collection contains no elements.
     */
    @Override
    public boolean isEmpty() {
        return !usable(header.successor());
    }

    /**
     * Returns the number of elements in this deque. If this deque contains more than <tt>Integer.MAX_VALUE</tt> elements, it returns <tt>Integer.MAX_VALUE</tt>.
     *
     * <p>
     * Beware that, unlike in most collections, this method is <em>NOT</em> a constant-time operation. Because of the asynchronous nature of these dequeue, determining the current number of elements requires traversing them all to count them.
     * Additionally, it is possible for the size to change during execution of this method, in which case the returned result will be inaccurate. Thus, this method is typically not very useful in concurrent applications.
     *
     * @return the number of elements in this deque.
     */
    @Override
    public int size() {
        long count = 0;
        for (Node<E> n = header.forward(); n != null; n = n.forward())
            ++count;
        return (count >= Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int) count;
    }

    /**
     * Removes the first element <tt>e</tt> such that <tt>o.equals(e)</tt>, if such an element exists in this deque. If the deque does not contain the element, it is unchanged.
     *
     * @param o element to be removed from this deque, if present.
     * @return <tt>true</tt> if the deque contained the specified element.
     * @throws NullPointerException if the specified element is <tt>null</tt>
     */
    @Override
    public boolean remove(Object o) {
        return removeFirstOccurrence(o);
    }

    /**
     * Appends all the elements in the specified collection to the end of this deque, in the order that they are returned by the specified collection's iterator. The behavior of this operation is undefined if the specified collection is modified
     * while the operation is in progress. (This implies that the behavior of this call is undefined if the specified Collection is this deque, and this deque is nonempty.)
     *
     * @param c the elements to be inserted into this deque.
     * @return <tt>true</tt> if this deque changed as a result of the call.
     * @throws NullPointerException if <tt>c</tt> or any element within it is <tt>null</tt>
     */
    @Override
    public boolean addAll(Collection<? extends E> c) {
        Iterator<? extends E> it = c.iterator();
        if (!it.hasNext())
            return false;
        do {
            addLast(it.next());
        } while (it.hasNext());
        return true;
    }

    /**
     * Removes all the elements from this deque.
     */
    @Override
    public void clear() {
        while (pollFirst() != null)
            ;
    }

    /**
     * Returns an array containing all the elements in this deque in the correct order.
     *
     * @return an array containing all the elements in this deque in the correct order.
     */
    @Override
    public Object[] toArray() {
        return toArrayList().toArray();
    }

    /**
     * Returns an array containing all the elements in this deque in the correct order; the runtime type of the returned array is that of the specified array. If the deque fits in the specified array, it is returned therein. Otherwise, a new array
     * is allocated with the runtime type of the specified array and the size of this deque.
     * <p>
     * <p>
     * If the deque fits in the specified array with room to spare (i.e., the array has more elements than the deque), the element in the array immediately following the end of the collection is set to null. This is useful in determining the length
     * of the deque <i>only</i> if the caller knows that the deque does not contain any null elements.
     *
     * @param a the array into which the elements of the deque are to be stored, if it is big enough; otherwise, a new array of the same runtime type is allocated for this purpose.
     * @return an array containing the elements of the deque.
     * @throws ArrayStoreException  if the runtime type of is not a supertype of the runtime type of every element in this deque.
     * @throws NullPointerException if the specified array is null.
     */
    @Override
    public <T> T[] toArray(@Nonnull T[] a) {
        return toArrayList().toArray(a);
    }

    /**
     * Returns a weakly consistent iterator over the elements in this deque, in first-to-last order. The <tt>next</tt> method returns elements reflecting the state of the deque at some point at or since the creation of the iterator. The method does
     * <em>not</em> throw {@link ConcurrentModificationException}, and may proceed concurrently with other operations.
     *
     * @return an iterator over the elements in this deque
     */
    @Override
    public Iterator<E> iterator() {
        return new CLDIterator();
    }

    final class CLDIterator implements Iterator<E> {
        Node<E> last;

        Node<E> next = header.forward();

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public E next() {
            Node<E> l = last = next;
            if (l == null)
                throw new NoSuchElementException();
            next = next.forward();
            return l.element;
        }

        @Override
        public void remove() {
            Node<E> l = last;
            if (l == null)
                throw new IllegalStateException();
            while (!l.delete() && !l.isDeleted())
                ;
        }
    }

}

