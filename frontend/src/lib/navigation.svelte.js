export let page = $state({
    component: "Home",
    props: {}
});

/**
 * Change the current page.
 * @param {Object} data - Either a payload {component, props} or a URL string.
 * @param {String} url - Either a payload {component, props} or a URL string.
 */
export async function renderPage(data, url) {
    console.log('RENDER', data, url)
    page.component = data.component;
    page.props = data.props;

    if (url.length > 0) {
        window.history.pushState({}, "", url);
    }
}
