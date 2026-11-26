package com.zifang.z.etl.web.config;

import java.io.IOException;
import jakarta.servlet.DispatcherType;
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
 * /etl 前缀兼容：让「集群裸转发（Traefik 不剥前缀）」与「本地 nginx 剥前缀」两种形态
 * 用同一套 controllers/static。
 *
 * <p>两条铁律（2026-11-26 生产 500 事故换来的）：
 * <ul>
 *   <li><b>只在 REQUEST 分发时重写</b>——welcome-page 与错误页都靠 forward 机制，
 *       forward 带着改过的 URI 进 Tomcat 的 {@code ApplicationHttpRequest} 会触发
 *       属性同步无限递归（StackOverflow → Whitelabel 500）；FORWARD / ERROR 分发一律放行原始请求；</li>
 *   <li><b>裸前缀 /etl、/etl/ 不 rewrite，改 302 重定向</b>到 /etl/index.html——
 *       rewrite 成 / 会撞上 welcome-page forward（同上），重定向把事情变成最朴素的 GET。</li>
 * </ul>
 *
 * @author zifang
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class EtlPrefixRewriter extends OncePerRequestFilter {

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        if (request.getDispatcherType() != DispatcherType.REQUEST) {
            chain.doFilter(request, response);
            return;
        }
        String uri = request.getRequestURI();
        if ("/etl".equals(uri) || "/etl/".equals(uri)) {
            response.sendRedirect("/etl/index.html");
            return;
        }
        String rewritten = rewrittenUri(uri);
        if (rewritten == null) {
            chain.doFilter(request, response);
            return;
        }
        chain.doFilter(new RewrittenRequest(request, rewritten), response);
    }

    /** /etl/xxx → /xxx；裸 /etl、/etl/（走重定向）与其余路径返回 null 不改写。 */
    static String rewrittenUri(String uri) {
        if (uri != null && uri.startsWith("/etl/") && !"/etl/".equals(uri)) {
            return uri.substring("/etl".length());
        }
        return null;
    }

    private static class RewrittenRequest extends HttpServletRequestWrapper {
        private final String uri;
        RewrittenRequest(HttpServletRequest request, String uri) {
            super(request);
            this.uri = uri;
        }
        @Override public String getRequestURI() { return uri; }
        @Override public String getServletPath() { return uri; }
        @Override public String getPathInfo() { return null; }
    }
}
