export const TimestampFormat = {
    Ago: false,
    DateTime: true,

    settingsKey: "settings.deals.timestamp-format",

    load: () => {
        const saved = localStorage.getItem(TimestampFormat.settingsKey)
        return JSON.parse(saved) || false
    },

    save: (val) => {
        localStorage.setItem(TimestampFormat.settingsKey, JSON.stringify(val));
    }
}

