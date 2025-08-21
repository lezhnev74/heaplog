// lib/router-utils.js
export function compileRoute(path) {
    const keys = [];
    const pattern = path
        .replace(/:([^/]+)/g, (_, key) => {
            keys.push(key);
            return "([^/]+)";
        })
        .replace(/\//g, "\\/");
    return {
        regex: new RegExp(`^${pattern}$`),
        keys
    };
}

export function matchRoute(route, pathname) {
    const match = pathname.match(route.regex);
    if (!match) return null;

    const params = {};
    route.keys.forEach((key, i) => {
        params[key] = decodeURIComponent(match[i + 1]);
    });

    return params;
}
