#!/usr/bin/env node
const { redrive } = require('./lib');
const joi = require('joi');
const schema = joi.object({
  from: joi
    .string()
    .uri()
    .required(),
  to: joi
    .string()
    .uri()
    .required(),
  attribute: joi.string()
});

const validateParams = params => {
  const { error } = schema.validate(params);
  if (error) {
    throw error;
  }
  return true;
};

if (require.main === module) {
  (async function() {
    const [from, to, attribute] = process.argv.slice(2);
    const params = { from, to, attribute };
    if (validateParams(params)) await redrive(params);
  })();
}
