export interface SignupEvent {
  userId: number;
  action: "signup";
}

export function parseSignupEvent(content: Buffer): SignupEvent {
  const value: unknown = JSON.parse(content.toString());

  if (typeof value !== "object" || value === null) {
    throw new Error("Invalid signup event");
  }

  const event = value as Record<string, unknown>;
  if (
    typeof event.userId !== "number" ||
    !Number.isInteger(event.userId) ||
    event.action !== "signup"
  ) {
    throw new Error("Invalid signup event");
  }

  return { userId: event.userId, action: "signup" };
}
