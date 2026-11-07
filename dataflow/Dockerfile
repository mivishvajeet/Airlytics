FROM gcr.io/dataflow-templates-base/python311-template-launcher-base:latest

ARG WORKDIR=/template
WORKDIR ${WORKDIR}

ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="${WORKDIR}/requirements.txt"
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/dataflow/pipeline.py"

COPY dataflow/requirements.txt ${WORKDIR}/requirements.txt
COPY dataflow/ ${WORKDIR}/dataflow/

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r ${WORKDIR}/requirements.txt \
    && pip download --no-cache-dir --dest /opt/dataflow/resources/ -r ${WORKDIR}/requirements.txt

ENV PYTHONPATH="${WORKDIR}"

ENTRYPOINT ["/opt/google/dataflow/python_template_launcher"]

