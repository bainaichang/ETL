package core;

import core.flowdata.RowSetTable;
import core.intf.IChannel;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

public class Channel implements IChannel<Object> {
    private RowSetTable header = null;
    private final Queue<Object> queue = new ConcurrentLinkedQueue();
    private final CopyOnWriteArrayList<Consumer<Object>> subscribers = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<Runnable> completionHooks = new CopyOnWriteArrayList<>();
    private volatile boolean closed = false;
    private final ExecutorService pool;

    public Channel(ExecutorService pool) {
        this.pool = pool;
    }

    @Override
    public RowSetTable getHeader() {
        return header;
    }

    @Override
    public void setHeader(RowSetTable header) {
        this.header = header;
    }

    @Override
    public void publish(Object row) {
        if (closed) {
            System.err.println("⚠️ Channel 已关闭，丢弃数据: " + row);
            return;
        }
        queue.offer(row);
        for (Consumer<Object> consumer : subscribers) {
            pool.submit(() -> consumer.accept(row));
        }
    }

    @Override
    public void onReceive(Consumer<Object> handler, Runnable onComplete) {
        this.subscribers.add(handler);
        if (onComplete != null) {
            this.completionHooks.add(onComplete);
        }

        Object row;
        while ((row = queue.poll()) != null) {
            final Object r = row;
            pool.submit(() -> handler.accept(r));
        }

        if (closed) {
            for (Runnable hook : completionHooks) {
                pool.submit(hook);
            }
        }
    }


    @Override
    public void subscribe(Consumer<Object> consumer) {
        this.onReceive(consumer, null);
    }

    @Override
    public synchronized void close() {
        if (!closed) {
            closed = true;
            System.out.println("✅ Channel 已关闭");
            for (Runnable hook : completionHooks) {
                pool.submit(hook);
            }
        }
    }

    @Override
    public boolean isClosed() {
        return closed;
    }
}