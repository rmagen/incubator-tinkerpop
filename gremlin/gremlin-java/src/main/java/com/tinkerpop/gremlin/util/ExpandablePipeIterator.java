package com.tinkerpop.gremlin.util;

import com.tinkerpop.gremlin.Holder;
import com.tinkerpop.gremlin.Pipe;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ExpandablePipeIterator<E> implements Iterator<Holder<E>> {

    private final ExpandableIterator<Holder<E>> expander = new ExpandableIterator<>();
    private Pipe<?, E> hostPipe = EmptyPipe.instance();

    public ExpandablePipeIterator(final Pipe<?, E> hostPipe) {
        this.hostPipe = hostPipe;
    }

    public void clear() {
        this.expander.clear();
    }

    public boolean hasNext() {
        return this.hostPipe.getPreviousPipe().hasNext() || this.expander.hasNext();
    }

    public Holder<E> next() {
        if (this.hostPipe.getPreviousPipe().hasNext())
            return (Holder<E>) this.hostPipe.getPreviousPipe().next();
        else
            return this.expander.next();
    }

    public void add(final Iterator<E> iterator) {
        this.expander.add((Iterator) iterator);
    }

    public class ExpandableIterator<T> implements Iterator<T> {

        private final Queue<Iterator<T>> queue = new LinkedList<>();

        public void clear() {
            this.queue.clear();
        }

        public boolean hasNext() {
            for (final Iterator<T> itty : this.queue) {
                if (itty.hasNext())
                    return true;
            }
            return false;
        }

        public T next() {
            while (true) {
                final Iterator<T> itty = this.queue.element();
                if (null != itty && itty.hasNext()) return itty.next();
                else this.queue.remove();
            }
        }

        public void add(final Iterator<T> iterator) {
            this.queue.add(iterator);
        }
    }
}
