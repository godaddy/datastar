

module.exports = {
  id: '00000000-0000-0000-0000-000002588490',
  name: 'Fido',
  color: 'brown',
  weight: 75,
  dogThing: 'hello',
  owner: JSON.stringify({
    name: 'John Doe',
    address: {
      street: '123 Somewhere Lane',
      locality: 'Chandler',
      region: 'Arizona',
      country: 'United States',
      code: '12345'
    }
  }),
  vaccinations: [
    JSON.stringify({ date: new Date('2015-09-09'), types: ['rabies', 'heartworms'] })
  ]
};
