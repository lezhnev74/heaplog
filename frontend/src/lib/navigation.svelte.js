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
    window.history.pushState(data, "", url);
    page.component = data.component;
    page.props = data.props;
}
