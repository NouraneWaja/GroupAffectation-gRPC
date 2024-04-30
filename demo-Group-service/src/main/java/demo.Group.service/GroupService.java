package demo.Group.service;
import java.util.concurrent.TimeUnit;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import net.devh.boot.grpc.server.serverfactory.GrpcServerConfigurer;


@SpringBootApplication

public class GroupService {

	public static void main(String[] args) {
		SpringApplication.run(GroupService.class, args);
	}

	@Bean
	public GrpcServerConfigurer keepAliveServerConfigurer() {
		return serverBuilder -> {
			if (serverBuilder instanceof NettyServerBuilder) {
				((NettyServerBuilder) serverBuilder).keepAliveTime(30, TimeUnit.SECONDS)
						.keepAliveTimeout(5, TimeUnit.SECONDS).permitKeepAliveWithoutCalls(true);
			}
		};
	}
}
