import uvicorn
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, RedirectResponse

app = FastAPI()


@app.get("/")
def root():
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>HTML Response</title>
    </head>
    <body>
        <h1>Hello, this is an HTML response!</h1>
        <p>This is some content in a paragraph.</p>
        <a href='http://localhost/test'> hi </a>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)


@app.get("/test")
def test():
    return RedirectResponse(url="http://example.com/", status_code=302)


uvicorn.run(app, host="0.0.0.0", port=80)
