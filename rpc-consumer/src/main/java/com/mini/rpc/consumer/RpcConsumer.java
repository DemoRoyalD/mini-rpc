package com.mini.rpc.consumer;

import com.mini.rpc.codec.MiniRpcDecoder;
import com.mini.rpc.codec.MiniRpcEncoder;
import com.mini.rpc.common.MiniRpcRequest;
import com.mini.rpc.common.RpcServiceHelper;
import com.mini.rpc.common.ServiceMeta;
import com.mini.rpc.handler.RpcResponseHandler;
import com.mini.rpc.protocol.MiniRpcProtocol;
import com.mini.rpc.provider.registry.RegistryService;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class RpcConsumer {
    private final Bootstrap bootstrap;
    private final EventLoopGroup eventLoopGroup;
    private ChannelFuture channelFuture;
    public static final Map<String, RpcConsumer> clientChannelFutureHolder = new ConcurrentHashMap<>(16);
    public static final Map<ChannelFuture, RpcConsumer> channelFutureHolder = new ConcurrentHashMap<>(16);

    public RpcConsumer() {
        bootstrap = new Bootstrap();
        eventLoopGroup = new NioEventLoopGroup(4);
        bootstrap.group(eventLoopGroup).channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel socketChannel) throws Exception {
                        socketChannel.pipeline()
                                .addLast(new MiniRpcEncoder())
                                .addLast(new MiniRpcDecoder())
                                .addLast(new RpcResponseHandler());
                    }
                });
    }

    public Bootstrap getBootstrap() {
        return this.bootstrap;
    }

    public ChannelFuture getChannelFuture(){
        return this.channelFuture;
    }

    public void setChannelFuture(ChannelFuture channelFuture){
        this.channelFuture = channelFuture;
    }

    public static ChannelFuture getChannelFuture(String ip, int port) throws InterruptedException {
        String key = String.join("#", ip, String.valueOf(port));
        ChannelFuture channelFuture = clientChannelFutureHolder.get(key).getChannelFuture();
        if (!Objects.isNull(channelFuture)) return channelFuture;
        // 构建新的consumer
        RpcConsumer rpcConsumer = new RpcConsumer();
        channelFuture = rpcConsumer.getBootstrap().connect(ip, port).sync();
        rpcConsumer.setChannelFuture(channelFuture);
        clientChannelFutureHolder.put(key, rpcConsumer);
        channelFutureHolder.put(channelFuture, rpcConsumer);
        channelFuture.channel().closeFuture().sync();
        return channelFuture;
    }


    public static void sendRequest(MiniRpcProtocol<MiniRpcRequest> protocol, RegistryService registryService) throws Exception {
        MiniRpcRequest request = protocol.getBody();
        Object[] params = request.getParams();
        String serviceKey = RpcServiceHelper.buildServiceKey(request.getClassName(), request.getServiceVersion());

        // hashCode是为了配合注册中心的 balanceLoad
        int invokerHashCode = params.length > 0 ? params[0].hashCode() : serviceKey.hashCode();
        // 通过zookeeper 注册中心获取 provider信息
        ServiceMeta serviceMetadata = registryService.discovery(serviceKey, invokerHashCode);

        if (serviceMetadata != null) {
            ChannelFuture future = getChannelFuture(serviceMetadata.getServiceAddr(), serviceMetadata.getServicePort());
            // 设置回调函数
            future.addListener((ChannelFutureListener) arg0 -> {
                if (future.isSuccess()) {
                    log.info("connect rpc server {} on port {} success.", serviceMetadata.getServiceAddr(), serviceMetadata.getServicePort());
                } else {
                    log.error("connect rpc server {} on port {} failed.", serviceMetadata.getServiceAddr(), serviceMetadata.getServicePort());
                    future.cause().printStackTrace();
                    RpcConsumer rpcConsumer = channelFutureHolder.remove(future);
                    if(!Objects.isNull(rpcConsumer)) {
                        rpcConsumer.eventLoopGroup.shutdownGracefully();
                        clientChannelFutureHolder.remove(String.join("#",serviceMetadata.getServiceAddr(), String.valueOf(serviceMetadata.getServicePort())));
                    }
                }
            });
            // 发送请求
            future.channel().writeAndFlush(protocol);
        }
    }
}
