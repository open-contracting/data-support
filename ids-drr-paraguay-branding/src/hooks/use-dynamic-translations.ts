"use client";

import { useTranslations } from "next-intl";

// See ids-drr-frontend's hooks/use-dynamic-translations.ts.
export function useDynamicTranslations(namespace?: string) {
    // biome-ignore lint/suspicious/noExplicitAny: Type check
    return (useTranslations as any)(namespace);
}
