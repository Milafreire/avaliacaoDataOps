[
  {
    '$lookup': {
      'from': 'Montadoras', 
      'localField': 'Montadora', 
      'foreignField': 'Montadora', 
      'as': 'Montadora_info'
    }
  }, {
    '$unwind': {
      'path': '$Montadora_info', 
      'includeArrayIndex': 'string', 
      'preserveNullAndEmptyArrays': true
    }
  }, {
    '$project': {
      'Carro': 1, 
      'Pais': '$Montadora_info.Pais'
    }
  }, {
    '$group': {
      '_id': '$Montadora_info.Pais', 
      'Carros': {
        '$push': '$$ROOT'
      }
    }
  }
]