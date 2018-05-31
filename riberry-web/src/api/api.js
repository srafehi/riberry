export const getSession = (name) => document.cookie.replace(new RegExp('(?:(?:^|.*;\\s*)' + name + '\\s*\\=\\s*([^;]*).*$)|^.*$'), "$1");
export const setSession = (name, value) => document.cookie = `${name}=${value}`;

export const TOKEN_NAME = 'token';
const URL = "http://localhost:5000";

window.getSession = getSession;
window.setSession = setSession;

const expandResource = (resource, expansionList) => (
    `${resource}${Array.isArray(expansionList) && expansionList.length ? '?expand=' + expansionList.join() : ''}`
);

export const fetchApi = ({auth = true, method, resource, mixin = {}, mixinHeaders = {}, expectResponse = true}) => {
    if (auth) {
        mixinHeaders = {...mixinHeaders, Authorization: `Bearer ${getSession(TOKEN_NAME)}`}
    }
    return fetch(`${URL}/${resource}`, {
        ...mixin,
        method,
        mode: 'cors',
        headers: {
            'Content-Type': 'application/json',
            ...mixinHeaders,
        }
    })
        .then(response => expectResponse ? response.json() : null);
};


export const postSession = (username, password) => fetchApi({
    auth: false,
    method: 'POST',
    resource: 'auth/token',
    mixin: {
        body: JSON.stringify({
            username,
            password
        })
    }
});

const sessions = {
    get: getSession,
    set: setSession,
    post: postSession
};

const self = {
    profile: ({expand=['details']}={}) => fetchApi({method: 'GET', resource: expandResource('self/', expand)})
};


const forms = {
    getAll: ({expand}={}) => fetchApi({method: 'GET', resource: expandResource('forms/', expand)}),
    get: ({id, expand}={}) => fetchApi({method: 'GET', resource: expandResource(`forms/${id}`, expand)}),
};

const jobs = {
    summary: () => fetchApi({method: 'GET', resource: 'jobs/summary'}),
    create: ({formId, data}) => fetch(`${URL}/forms/${formId}/jobs`, {
        method: 'POST',
        mode: 'cors',
        body: data,
        headers: {
            Authorization: `Bearer ${getSession(TOKEN_NAME)}`
        }
    })
};

const api = {
    self,
    forms,
    sessions,
    jobs
};

export default api;