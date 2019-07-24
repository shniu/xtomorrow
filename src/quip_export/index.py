"""
Quip export to pdf
"""

import os
import pdfkit
import click

try:
    import quip
except ImportError:
    from . import quip

# define default config
default_access_token = "VEpKQU1BRHNRSFQ=|1538120541|vJGX9ki9/qOW9rOgcnOudd3WJOq20FzIzS52Z+pNWDg="
pdf_options = {
    'page-size': 'Letter',
    'margin-top': '0.25in',
    'margin-right': '0.25in',
    'margin-bottom': '0.25in',
    'margin-left': '0.25in',
    'encoding': "UTF-8",
    'no-outline': None
}


def export(access_token, thread_token):
    if not access_token or not thread_token:
        raise Exception("access_token and thread_token must not empty")

    client = quip.QuipClient(access_token=access_token)
    thread = client.get_thread(thread_token)

    htmlTpl = """<!DOCTYPE html>
<html lang="zh-cn">
<head>
    <title>{{title}}</title>

    <meta charset="utf-8" />
</head>
<body>
    <div class="document-content">
        <section class="section">
            {{text}}
        </section>
    </div>
</body>
</html>
    """

    title = thread['thread']['title']
    html = str(thread['html'])

    htmlTpl = htmlTpl.replace('{{title}}', title)
    htmlTpl = htmlTpl.replace('{{text}}', html)
    print(htmlTpl)
    return title, htmlTpl


def store_html_pdf(filename, content):
    if not os.path.exists(os.path.dirname(filename)):
        os.makedirs(os.path.dirname(filename))

    with open("%s.html" % filename, 'w', encoding='utf-8') as f:
        f.write(content)

    pdfkit.from_string(content, "%s.pdf" % filename, options=pdf_options, css="style.css")  # , options=pdf_options


@click.command()
@click.option('--access_token', default="", help='Your access_token of quip.')
@click.option('--threads', default="oXtBAbFrTXna", help="thread id list, split by comma, e.g. 123,345")
@click.option('--save_path', default="", help="Save html and pdf to which place")
def cli_export(access_token, threads, save_path):
    click.echo('=' * 25)
    click.echo('Start export quip file to pdf or html\n')

    file_path = os.path.join(os.path.dirname(__file__), 'weekly')
    click.echo('file saved path is: %s' % file_path)

    if not access_token:
        access_token = default_access_token

    threads = threads.strip().split(',')
    for thread in threads:
        title, html = export(access_token, thread)

        store_html_pdf(os.path.join(file_path, title), html)


if __name__ == '__main__':
    cli_export()
