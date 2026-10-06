export class DomainError extends Error {
  constructor(message: string) {
    super(message);
    this.name = this.constructor.name;
  }
}

export class StaleEventError extends DomainError {
  constructor(message: string) {
    super(message);
  }
}

export class InvalidStateTransitionError extends DomainError {
  constructor(from: string, to: string) {
    super(`Invalid state transition from ${from} to ${to}`);
  }
}

export class RetryableEmailError extends DomainError {
  constructor(message: string, public readonly statusCode?: number) {
    super(message);
  }
}

export class PermanentEmailError extends DomainError {
  constructor(message: string, public readonly statusCode?: number) {
    super(message);
  }
}
