import os
from typing import Any
import jupytext
import papermill
import nbformat
from nbconvert.preprocessors import ExecutePreprocessor
from nbconvert import HTMLExporter


def __execute_notebook(
    notebook_file_path: str, timeout: int = 3600, kernel_name: str = "python3"
) -> tuple:
    with open(notebook_file_path) as notebook_file:
        nb = nbformat.read(notebook_file, as_version=4)
    ep = ExecutePreprocessor(timeout=timeout, kernel_name=kernel_name)
    ep.preprocess(
        nb, {"metadata": {"path": os.path.dirname(notebook_file_path)}}
    )
    with open(notebook_file_path, "w", encoding="utf-8") as notebook_file:
        nbformat.write(nb, notebook_file)

    html_exporter = HTMLExporter()

    return html_exporter.from_filename(notebook_file_path)


def generate_report(
    template_filename: str, output_filename: str, parameters: dict[str, Any]
) -> None:
    this_dir = os.path.dirname(os.path.realpath(__file__))
    notebook = jupytext.read(os.path.join(this_dir, template_filename))
    output_dir = os.path.dirname(output_filename)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    path_without_ext = os.path.splitext(os.path.basename(output_filename))[0]
    nb_out_path = os.path.join(output_dir, f"{path_without_ext}.ipynb")
    html_out_path = os.path.join(output_dir, f"{path_without_ext}.html")
    jupytext.write(notebook, nb_out_path)

    papermill.execute_notebook(
        nb_out_path,
        nb_out_path,
        prepare_only=True,
        parameters=parameters,
    )

    body, resources = __execute_notebook(nb_out_path)

    with open(html_out_path, "w", encoding="utf-8") as html_output_file:
        html_output_file.write(body)
