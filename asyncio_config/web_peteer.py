from pyppeteer import launch
import asyncio

class Web_peteer(object):

    async def web_peteer(self, url, proxy, timeout):
        args = ['--no-sandbox', '--disable-infobars']
        if proxy:
            args = ['--no-sandbox', '--disable-infobars', f'--proxy-server={proxy}']
        browser = await launch({'headless': True, 'args': args,
                                'handleSIGINT': False, 'handleSIGTERM': False, 'handleSIGHUP': False})
        page = await browser.newPage()
        await page.setJavaScriptEnabled(enabled=True)
        response = await page.goto(url, options={'timeout': timeout*2000})
        await asyncio.sleep(5)
        html = await page.content()
        cookies = await page.cookies()
        await browser.close()
        return html, cookies, response.status


if __name__ == '__main__':
    web_peteer = Web_peteer()
    url = 'http://www.pudong.gov.cn/shpd/CateogryPaging/019004006/Default_3.htm'
    # url = 'http://www.pudong.gov.cn/shpd/department/019004/019004006/'
    html, cookies, status = asyncio.get_event_loop().run_until_complete(web_peteer.web_peteer(url, None, 15))
    print(html)
