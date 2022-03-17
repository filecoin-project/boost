export const DealsPerPage = {
    Default: 10,

    settingsKey: "settings.deals.per-page",

    load: () => {
        const saved = localStorage.getItem(DealsPerPage.settingsKey)
        console.log('load', typeof JSON.parse(saved))
        return JSON.parse(saved) || DealsPerPage.Default
    },

    save: (val) => {
        localStorage.setItem(DealsPerPage.settingsKey, JSON.stringify(val));
    }
}

