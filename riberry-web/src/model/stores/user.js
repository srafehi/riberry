import {flow, types} from "mobx-state-tree"
import api, {TOKEN_NAME} from "../../api/api";
import {User} from "../models";


const login = self => flow(function* (username, password) {
    const sessionResponse = yield api.sessions.post(username, password);
    if (!sessionResponse.error) {
        api.sessions.set(TOKEN_NAME, sessionResponse.data.token);
        yield self.loadUserProfile();
    }
});

const logout = self => () => {
    api.sessions.set(TOKEN_NAME, '');
    self.user = null;
};

const loadUserProfile = self => flow(function* () {
    const profileResponse = yield api.self.profile();
    if (profileResponse.data) {
        self.user = profileResponse.data;
    }
});

const setup = self => flow(function* () {
    const token = api.sessions.get(TOKEN_NAME);
    if (token) {
        yield self.loadUserProfile();
    }
    self.initialLoad = false;
});


const storeActions = self => ({
    login: login(self),
    logout: logout(self),
    loadUserProfile: loadUserProfile(self),
    setup: setup(self)
});


const loggedIn = self => Boolean(self.user);


const storeViews = self => ({
    get loggedIn() {
        return loggedIn(self);
    }
});


const UserStore = types.model({
    user: types.maybe(types.late(() => User)),
    initialLoad: true
}).actions(storeActions).views(storeViews);

export const userStore = UserStore.create({
    user: null
});

userStore.setup();