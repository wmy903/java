package org.aliyun.serverless.scaler;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Empty;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import org.aliyun.serverless.config.Config;
import org.aliyun.serverless.model.*;
import org.aliyun.serverless.platformClient.Client;
import org.aliyun.serverless.platformClient.PlatformClient;
import protobuf.SchedulerProto;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;
import java.time.*;
import java.util.function.Consumer;
import java.util.logging.Logger;

public class SimpleScaler implements Scaler {
    private static final Logger logger = Logger.getLogger(SimpleScaler.class.getName());
    private final Config config;     //several clase variables, are used to store configuration settings, function information
    private final Function function;
    private final Client platformClient;
    private final Lock mu;
    private final CountDownLatch wg;
    private final Map<String, Instance> instances;   //定义了键值对类型的类变量instances，通过string类型的键可以访问Instance值 (这里存的是正在运行的instance?)
    private final Deque<Instance> idleInstances;     //定义了一个Deque(double-ended queue)对象
    private final Map<String, Slot> slots;
    private final Deque<Slot> idleSlots;

    // initialize the class variables and dependencies based on the provided function and config parameters.
    public SimpleScaler(Function function, Config config) {
        try {
            this.config = config;
            this.function = function;
            this.platformClient = new PlatformClient(config.getPlatformHost(), config.getPlatformPort());
            this.mu = new ReentrantLock();
            this.wg = new CountDownLatch(1);
            this.instances = new ConcurrentHashMap<>();
            this.idleInstances = new LinkedList<>();
            this.slots = new ConcurrentHashMap<>();
            this.idleSlots = new LinkedList<>();
            logger.info(String.format("New scaler for app: %s is created", function.getKey()));
            new Thread(this::gcLoop).start();   //调研thread的用法
        } catch (Exception e) {
            throw new RuntimeException("failed to create Simple scaler", e);
        }
    }

    public void Assign(Context ctx, SchedulerProto.AssignRequest request, StreamObserver<SchedulerProto.AssignReply> responseObserver) throws Exception {
        /**
         * 原思路： 判断是否有空实例
         */
        Instant start = Instant.now();                              //构建instant类型变量start (Instant是java固有的类型)
        String instanceId = UUID.randomUUID().toString();
        try {
            logger.info("Start assign, request id: " + request.getRequestId());
            mu.lock();
            if (!idleInstances.isEmpty()) {                         //存在空闲instance
                Instance instance = idleInstances.pollFirst();      //移除indleInstances的第一个元素，并分配给instance
                instance.setBusy(true);                             //设置该实例为忙
                String instanceID = instance.getID();               //获取实例id
                instances.put(instanceID, instance);                //

                Slot slot = instance.getSlot();                     //得到slot
                String slotID = slot.getId();
                slots.put(slotID, slot);
                mu.unlock();

                logger.info(String.format("Finish assign, request id: %s, instance %s reused", request.getRequestId(), instanceID));

                // 从SchedulerProto protocol buffer构建了Assignment message的实例，我理解就是完成一个instance和request绑定的消息记录
                SchedulerProto.Assignment assignment = SchedulerProto.Assignment.newBuilder()
                        .setRequestId(request.getRequestId()).setMetaKey(instance.getMeta().getKey())   // <为什么类型是实例的类型？难道不需要实例类型和请求的类型相匹配吗？>
                        .setInstanceId(instanceID).build();
                responseObserver.onNext(SchedulerProto.AssignReply.newBuilder().setStatus(SchedulerProto.Status.Ok).setAssigment(assignment).build());  // 通过r.onNext()输出返回值
                responseObserver.onCompleted();
                return;
            }
            mu.unlock();

            // Create new instance
            SchedulerProto.ResourceConfig resourceConfig = SchedulerProto.ResourceConfig.newBuilder()
                    .setMemoryInMegabytes(request.getMetaData().getMemoryInMb()).build();    //按照request的资源需求分配资源
            SlotResourceConfig slotResourceConfig = new SlotResourceConfig(resourceConfig);

            if (!idleSlots.isEmpty()) {
                //Slot slot = idleSlots.pollFirst();
                ListenableFuture<Slot> slotFuture = platformClient.CreateSlot(ctx, request.getRequestId(), slotResourceConfig);
                Slot slot = slotFuture.get();   //创建一个slot，此时还只有与request有关的资源信息
            } else {
                ListenableFuture<Slot> slotFuture = platformClient.CreateSlot(ctx, request.getRequestId(), slotResourceConfig);
                Slot slot = slotFuture.get();   //创建一个slot，此时还只有与request有关的资源信息
            }

            //可否在此之前修改，提前新建好slot，此处只需要取就行

            SchedulerProto.Meta meta = SchedulerProto.Meta.newBuilder()
                    .setKey(request.getMetaData().getKey())
                    .setRuntime(request.getMetaData().getRuntime())
                    .setTimeoutInSecs(request.getMetaData().getTimeoutInSecs())
                    .build();
            Function function = new Function(meta);

            ListenableFuture<Instance> instanceFuture = platformClient.Init(ctx, request.getRequestId(), instanceId, slot, function);
            Instance instance = instanceFuture.get();   //初始化一个实例
            String instanceID = instance.getID();
            String slotID = slot.getId();

            mu.lock();
            instance.setBusy(true);
            instances.put(instanceID, instance);
            slots.put(slotID, slot);
            mu.unlock();

            logger.info(String.format("request id: %s, instance %s for app %s is created, init latency: %dms",
                    request.getRequestId(), instanceID, instance.getMeta().getKey(), instance.getInitDurationInMs()));
            SchedulerProto.Assignment assignment = SchedulerProto.Assignment.newBuilder()
                    .setRequestId(request.getRequestId()).setMetaKey(instance.getMeta().getKey())
                    .setInstanceId(instanceID).build();
            responseObserver.onNext(SchedulerProto.AssignReply.newBuilder().setStatus(SchedulerProto.Status.Ok).setAssigment(assignment).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            String errorMessage = String.format("Failed to assign instance, request id: %s due to %s", request.getRequestId(), e.getMessage());
            logger.info(errorMessage);
            responseObserver.onError(new RuntimeException(errorMessage, e));
        } finally {
            logger.info(String.format("Finish assign, request id: %s, instance id: %s, cost %dms",
                    request.getRequestId(), instanceId, Duration.between(start, Instant.now()).toMillis()));
        }
    }

    public void Idle(Context ctx, SchedulerProto.IdleRequest request, StreamObserver<SchedulerProto.IdleReply> responseObserver) throws Exception {
        /**
         * idle过程：先将实例置空，返回Idle结果；再进一步判断能否销毁，如可以，则deleteslot
         */
        if (!request.getAssigment().isInitialized()) {         //确认请求对象的assignment字段是否初始化，判断request是否已分配assignment
            responseObserver.onError(new RuntimeException("assignment is null"));
            return;
        }

        SchedulerProto.IdleReply.Builder replyBuilder = SchedulerProto.IdleReply.newBuilder()
                .setStatus(SchedulerProto.Status.Ok);
        long start = System.currentTimeMillis();
        String instanceId = request.getAssigment().getInstanceId();      //获得分配给该请求的实例id
        try {
            logger.info(String.format("Start idle, request id: %s", request.getAssigment().getRequestId()));
            boolean needDestroy = false;
            boolean slotneedDestroy = false;
            String slotId = "";
            if (request.getResult().isInitialized() && request.getResult().getNeedDestroy()) {   //判断实例是否可以销毁
                needDestroy = true;
            }

            mu.lock();
            Instance instance = instances.get(instanceId);    //根据该实例ID,从实例队列中找到实例
            if (instance == null) {                           //如果实例不存在
                mu.unlock();
                responseObserver.onError(new RuntimeException(
                        String.format("request id %s, instance %s not found",
                                request.getAssigment().getRequestId(), instanceId)));
                return;
            }

            instance.setLastIdleTime(LocalDateTime.now());    //设置实例空闲时间
            if (!instance.getBusy()) {
                mu.unlock();
                logger.warning(String.format("request id %s, instance %s already freed",
                        request.getAssigment().getRequestId(), instanceId));
            } else {
                instance.setBusy(false);
                idleInstances.offerFirst(instance);   // 实例被设置空闲后，进入到空闲队列
                mu.unlock();
            }

            responseObserver.onNext(replyBuilder.build());    // 返回idleReply
            responseObserver.onCompleted();
            if (needDestroy) {
                deleteSlot(ctx, instance, request.getAssigment().getRequestId(), "bad instance");
            }
        } catch (Exception e) {
            String errorMessage = String.format("idle failed with: %s", e.getMessage());
            logger.info(errorMessage);
            responseObserver.onNext(replyBuilder
                    .setStatus(SchedulerProto.Status.InternalError)
                    .setErrorMessage(errorMessage)
                    .build());
            responseObserver.onCompleted();
        } finally {
            long cost = System.currentTimeMillis() - start;
            logger.info(String.format("Idle, request id: %s, instance: %s, cost %dus%n",
                    request.getAssigment().getRequestId(), instanceId, cost));  // 记录整个idle()过程的时间
        }
    }

    private void deleteSlot(Context context, Instance instance, String requestId, String reason) {
        /**
         * 先删除instance（从instances和idleInstances中删除），再销毁slot
         */
        String instanceID = instance.getID();
        String slotID = instance.getSlot().getId();
        String metaKey = instance.getMeta().getKey();
        logger.info(String.format("start delete Instance %s (Slot: %s) of app: %s", instanceID, slotID, metaKey));

        mu.lock();
        if (!instances.containsKey(instanceID)) {
            mu.unlock();
            return;
        }
        instances.remove(instanceID);
        idleInstances.remove(instance);
        mu.unlock();

        boolean slotneedDestroy = false;
        try {
            if (slotneedDestroy) {
                ListenableFuture<Empty> future = platformClient.DestroySLot(context, requestId, slotID, reason);
                future.get();
            } else {
                //还需要消除slot在实例初始化时的一些元数据
                Slot slot = instance.getSlot();
                idleSlots.offerFirst(slot);
            }

        } catch (Exception e) {
            logger.info(String.format("delete Instance %s (Slot: %s) of app: %s failed with: %s",
                    instanceID, slotID, metaKey, e.getMessage()));
        }
    }

    private void gcLoop() {
        logger.info(String.format("gc loop for app: %s is starting", function.getKey()));
        Timer timer = new Timer();
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                mu.lock();

                Iterator<Instance> iterator = idleInstances.iterator();
                while (iterator.hasNext()) {
                    Instance instance = iterator.next();
                    if (instance != null) {
                        long idleDuration = Duration.between(instance.getLastIdleTime(), LocalDateTime.now()).toMillis();
                        if (idleDuration > config.getIdleDurationBeforeGC().toMillis()) {
                            String reason = String.format("Idle duration: %dms, exceed configured duration: %dms",
                                    idleDuration, config.getIdleDurationBeforeGC().toMillis());
                            CompletableFuture.runAsync(() -> deleteSlot(Context.current(), instance, UUID.randomUUID().toString(), reason));
                            logger.info(String.format("Instance %s of app %s is GCed due to idle for %dms",
                                    instance.getID(), instance.getMeta().getKey(), idleDuration));
                        }
                    }
                }
                mu.unlock();
            }
        };

        timer.schedule(task, 0, this.config.getGcInterval().toMillis());
    }

    public Stats Stats() {
        mu.lock();
        Stats stats = new Stats();
        stats.setTotalInstance(instances.size());
        stats.setTotalIdleInstance(idleInstances.size());
        mu.unlock();

        return stats;
    }
}