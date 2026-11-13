FROM python:3.15-slim

ARG UID=1000
ARG GID=1000

RUN set -e; \
    if [ "${GID}" -ne 0 ] && ! getent group "${GID}" >/dev/null; then \
        groupadd -g "${GID}" appuser; \
    fi && \
    if [ "${UID}" -ne 0 ] && ! getent passwd "${UID}" >/dev/null; then \
        useradd -m -u "${UID}" -g "${GID}" -d /home/appuser appuser; \
    fi

WORKDIR /app

COPY pyproject.toml README.md LICENSE ./
COPY claude_chatter/ ./claude_chatter/
RUN pip install --no-cache-dir build flake8 pytest \
    && pip install --no-cache-dir .

COPY tests/ ./tests/

RUN mkdir -p /home/appuser/.claude/sessions /home/appuser/.claude/projects /app/dist /app/build \
    && chmod -R 1777 /tmp /app/dist /app/build \
    && if getent passwd appuser >/dev/null; then chown -R appuser:appuser /home/appuser; fi

USER appuser
ENV HOME=/home/appuser

CMD ["python", "-m", "unittest", "discover", "-s", "tests", "-p", "test_suite.py"]
