package com.zifang.z.etl.web.config;

import java.io.IOException;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import jakarta.servlet.http.HttpServletResponse;

import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * /etl 前缀兼容（2026-11-26 集群部署起）。
 *
 * <p>集群 Traefik 对 /etl/** 只做裸转发（生产上 strip middleware 注入不生效，见
 * z-opc 仓 08-etl-route.yaml 注释），控制台与 API 以 /etl 前缀到达本服务；本地 aio
 * 由 nginx 剥前缀后以裸路径到达。本过滤器把 /etl/** 一律 rewrite 成 /** 放行，
 * 两种形态共用同一套 controller 与静态资源，不再依赖外部剥前缀。
 *
 * <p>客户端侧不受影响：浏览器 pathname 仍是 /etl/...，控制台单文件 JS 按
 * location.pathname 前缀继续带 /etl 发请求，服务端对前缀请求同样 rewrite，闭环成立。
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class EtlPrefixRewriter extends OncePerRequestFilter {

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String uri = request.getRequestURI();
        String rewritten = rewrittenUri(uri);
        if (rewritten == null) {
            chain.doFilter(request, response);
            return;
        }
        chain.doFilter(new RewrittenRequest(request, rewritten), response);
    }

    /** 返回改写后的 URI；无需改写时返回 null（/etlx 等近似前缀不误伤）。 */
    static String rewrittenUri(String uri) {
        if (uri.startsWith("/etl/")) {
            return uri.substring("/etl".length());
        }
        if ("/etl".equals(uri) || "/etl/".equals(uri)) {
            return "/";
        }
        return null;
    }

    private static class RewrittenRequest extends HttpServletRequestWrapper {

        private final String uri;

        RewrittenRequest(HttpServletRequest request, String uri) {
            super(request);
            this.uri = uri;
        }

        @Override
        public String getRequestURI() {
            return uri;
        }

        @Override
        public String getServletPath() {
            return uri;
        }

        @Override
        public String getPathInfo() {
            return null;
        }
    }
}