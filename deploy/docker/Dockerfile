# z-etl-web 管控面运行镜像（2026-11-26 首次容器化）。
#
# 底座口径（照 z-opc 体系离线镜像纪律）：
#   · 产物是 Mac 上编好的 fat jar（跨架构字节码），只 COPY 不打包；
#   · 架构 = amd64（家庭 k3s 节点）——本机 arm64 拉不到 docker.io 底座，
#     构建前先 `docker pull --platform linux/amd64 docker.m.daocloud.io/library/eclipse-temurin:17-jre`
#     再 `docker tag` 成本地标签 `etl-jre17:amd64`，Dockerfile 只认本地标签。
#
# 构建（仓库根为上下文）：
#   docker build --platform linux/amd64 -f deploy/docker/Dockerfile \
#     -t zopc/z-etl-web:local .
# 灌集群：
#   docker save zopc/z-etl-web:local | ssh zifang@<node> \
#     "sudo ctr -a /run/k3s/containerd/containerd.sock -n k8s.io images import -"
#   （pod 落点的每台节点都要一份；imagePullPolicy: Never 不联网拉）
FROM etl-jre17:amd64

ENV TZ=Asia/Shanghai \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8

COPY z-etl-web/target/z-etl-web-1.0.0.jar /app/app.jar

# 数据面固定口径 = 阿里云 RDS（z.base.db.etl.*，缺省回落 z.base.db.default.*）；
# 表由 EtlSchema 启动幂等自建（etl_* 8 张），凭据全部走环境变量注入。
EXPOSE 8092

ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]