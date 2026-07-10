"use client";

import Image from "next/image";
import { Text } from "opub-ui";

import logo from "../assets/logo.svg";
import { useDynamicTranslations } from "../hooks/use-dynamic-translations";

export function Footer() {
  const t = useDynamicTranslations("footer");
  return (
    <footer className="flex flex-col items-center justify-between gap-3 bg-backgroundSolidDark px-5 py-6 text-textOnBGDefault md:flex-row md:px-10">
      <Image src={logo} alt="" className="h-6 w-auto" />
      <Text variant="bodySm" color="onBgDefault">
        {t("credit")}
      </Text>
    </footer>
  );
}
